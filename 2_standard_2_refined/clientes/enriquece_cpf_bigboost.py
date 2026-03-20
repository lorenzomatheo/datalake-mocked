# Databricks notebook source
get_ipython().system("pip install aiohttp --upgrade")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Enriquece CPF com dados do bigboost

# COMMAND ----------

import asyncio
import time

import aiohttp
import dateutil.parser
import pyspark.sql.types as T
import requests
from pyspark.context import SparkConf
from pyspark.sql import Row, SparkSession

from maggulake.enums import SexoCliente
from maggulake.utils.iters import limita_iterable_a_x_elementos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

# temos clientes refinados somente em prod
# dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# STAGE = dbutils.widgets.get("stage")
STAGE = "prod"
CATALOG = "production" if STAGE == "prod" else "staging"

tabela_clientes_conta_refined = f"{CATALOG}.refined.clientes_conta_refined"
tabela_produtos_refined = f"{CATALOG}.refined.produtos_refined"

# Salvar sempre em prod para não repetir o processamento
s3_bucket_name = "maggu-datalake-prod"
s3_file_path = f"s3://{s3_bucket_name}/3-refined-layer/cpfs_processados"

# COMMAND ----------

BIGDATACORP_ADDRESS = "https://plataforma.bigdatacorp.com.br/"
BIGDATACORP_TOKEN = dbutils.secrets.get(scope="databricks", key="BIGDATACORP_TOKEN")
URL = BIGDATACORP_ADDRESS + "peoplev2"
HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "AccessToken": BIGDATACORP_TOKEN,
}

# COMMAND ----------

config = SparkConf().setAll(pairs=[('spark.sql.caseSensitive', 'true')])

spark = (
    SparkSession.builder.appName('enriquece_cpf_bigboost')
    .config(conf=config)
    .getOrCreate()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ler clientes_conta_refined_df

# COMMAND ----------

clientes_conta_refined_df = spark.read.table(tabela_clientes_conta_refined)

clientes_conta_refined_df = clientes_conta_refined_df.filter(
    clientes_conta_refined_df.cpf_cnpj.isNotNull()
)

# refinar somente clientes que fizeram alguma compra atraves da maggu
clientes_conta_refined_df = clientes_conta_refined_df.filter(
    clientes_conta_refined_df.total_de_cestas > 1
)

# refinar somente pessoas_fisicas
clientes_conta_refined_df = clientes_conta_refined_df.filter(
    clientes_conta_refined_df.eh_pessoa_fisica == True
)

df = clientes_conta_refined_df.select(
    "cpf_cnpj",
    "nome_completo",
    "nome_social",
    "data_nascimento",
    "idade",
    "faixa_etaria",
    "sexo",
)

df = df.dropDuplicates(["cpf_cnpj"])

# COMMAND ----------

# retirar linhas em que idade não faz sentido
a_fazer_df = df.filter(
    (df.data_nascimento.isNull())
    | (df.sexo.isNull())
    | (df.nome_completo.isNull())
    | (df.idade < 0)  # idade negativa está errado
    | (df.idade > 105)  # dificilmente temos idades maiores de 105 anos
)

# retirar linhas em que idade esteja entre 0 e 17 anos
# Bigboost não permite consultar menores de idade
a_fazer_df = a_fazer_df.filter(
    (a_fazer_df.idade >= 18)
    | (a_fazer_df.idade < 0)
    | (a_fazer_df.idade.isNull())
    | (a_fazer_df.idade > 105)
)


print("Quantidade de itens a_fazer_df: ", a_fazer_df.count())

# COMMAND ----------

# a_fazer_df.filter(a_fazer_df.idade < 18).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar DataFrame de CPFs já processados

# COMMAND ----------

already_processed_df = spark.read.parquet(s3_file_path).select("cpf_cnpj").distinct()

# Realizar o anti join para obter apenas os CPFs que não foram processados
a_fazer_df = a_fazer_df.join(already_processed_df, on=["cpf_cnpj"], how="leftanti")

print("Quantidade de itens já processados: ", already_processed_df.count())

# COMMAND ----------

cpf_list = [row["cpf_cnpj"] for row in a_fazer_df.select("cpf_cnpj").collect()]

print("Quantidade de CPFs a processar: ", len(cpf_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função assíncrona em batches

# COMMAND ----------


async def get_data_from_api_async(cpf, session):
    payload = {"Datasets": "basic_data", "q": f"doc{{{cpf}}}"}
    for attempt in range(max_retries):
        try:
            async with session.post(
                URL, headers=HEADERS, json=payload, timeout=5
            ) as response:
                response.raise_for_status()
                if response.status == 200:
                    result = await response.json()
                    status = result["Status"]
                    if "basic_data" in status:
                        status = status["basic_data"][0]["Code"]
                    else:
                        print(f"Erro na resposta para CPF {cpf}: {result}")
                        errors_dict[cpf] = result
                        return None, None, None

                    if int(status) != 0:
                        print(f"Erro na resposta para CPF {cpf}: {result}")
                        errors_dict[cpf] = result
                        return None, None, None

                    data = result.get("Result", [{}])[0].get("BasicData")
                    if not data:
                        print("\t Data não encontrado - CPF: ", cpf, result)
                        errors_dict[cpf] = result
                        return None, None, None

                    nome = data.get('Name')

                    match data.get('Gender'):
                        case 'M':
                            genero = SexoCliente.MASCULINO.value
                        case 'F':
                            genero = SexoCliente.FEMININO.value
                        case _:
                            genero = None

                    try:
                        data_nascimento = dateutil.parser.parse(data.get('BirthDate'))
                        data_nascimento = data_nascimento.date()
                    except (TypeError, dateutil.parser.ParserError):
                        data_nascimento = None

                    print("CPF concluído com sucesso! ", cpf)
                    return nome, genero, data_nascimento
                else:
                    print(f"Erro na requisição para CPF {cpf}: {response.status}")
                    errors_dict[cpf] = response.text
        except requests.exceptions.RequestException as e:
            print(f"Erro na tentativa {attempt + 1} para CPF {cpf}: {e}")
            errors_dict[cpf] = e
            await asyncio.sleep(2**attempt)  # Exponential backoff
    return None, None, None


async def process_batch(batch, session):
    tasks = [get_data_from_api_async(cpf, session) for cpf in batch]
    results = await asyncio.gather(*tasks)
    return results


async def main(cpf_list):
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(cpf_list), batch_size):
            # Executar batch
            batch = cpf_list[i : i + batch_size]
            results = await process_batch(batch, session)

            # processar resultados do batch
            for cpf, (nome, genero, data_nascimento) in zip(batch, results):
                if nome and genero and data_nascimento:
                    results_dict[cpf] = (nome, genero, data_nascimento)
                else:
                    errors_list.append(cpf)

            print(f"Processados {i + batch_size} de {n_cpfs}")
            time.sleep(5)  # evitando rate limit


# COMMAND ----------

# Executar o processamento assíncrono
batch_size = 100
max_retries = 2
errors_list = []
errors_dict = {}
results_dict = {}

n_cpfs = len(cpf_list)

# limitar para 10000 para não explodir o custo
# (cada consulta custa R$0.03 no bigboost)
cpf_list = limita_iterable_a_x_elementos(cpf_list, 10000)

await main(cpf_list)

# COMMAND ----------

print("quantidade de CPFs que não puderam ser preenchidos: ", len(errors_list))
print("quantidade de CPFs preenchidos: ", len(results_dict))

success_rate = len(results_dict) / n_cpfs * 100
print(f"taxa de sucesso: {success_rate:.2f} %")

# COMMAND ----------

menores_de_idade = {}
menores_de_doze = {}

for key, value in errors_dict.items():
    if not isinstance(value, dict):
        continue
    if "Status" not in value:
        continue
    status = value["Status"]

    error = {}

    if isinstance(status, dict):
        for key2, value2 in status.items():
            if isinstance(value2, list):
                error = value2[0]
    code = error["Code"]
    message = error["Message"]
    if (
        message
        == "THIS CPF BELONGS TO A MINOR. DATE OF BIRTH IS NEEDED TO PROCESS REQUEST"
        or code == -200
    ):
        menores_de_idade[key] = message
        continue

    if (
        message
        == "THIS CPF BELONGS TO A 12 YEAR OLD OR LESS. REQUEST CAN'T BE PROCESSED FOR LEGAL REASONS."
        or code == -201
    ):
        menores_de_doze[key] = message
        continue

print(
    "Um total de ",
    len(menores_de_idade),
    " CPFs pertencem a pessoas menores de 12 anos",
)

# COMMAND ----------

# Convert results_dict to a spark dataframe

# bb = big boost
schema = T.StructType(
    [
        T.StructField("cpf_cnpj", T.StringType(), False),
        T.StructField("nome_completo_bb", T.StringType(), True),
        T.StructField("sexo_bb", T.StringType(), True),
        T.StructField("data_nascimento_bb", T.DateType(), True),
        T.StructField("eh_menor_doze_bb", T.BooleanType(), True),
        T.StructField("eh_menor_idade_bb", T.BooleanType(), True),
    ]
)

rows = [
    Row(
        cpf_cnpj=cpf,
        nome_completo=data[0],
        sexo_bb=data[1],
        data_nascimento_bb=data[2],
        eh_menor_doze_bb=False,
        eh_menor_idade_bb=False,
    )
    for cpf, data in results_dict.items()
]

# adiciona cpfs de menores de idade e menores de doze anos
rows += [
    Row(
        cpf_cnpj=cpf,
        nome_completo=None,
        sexo_bb=None,
        data_nascimento_bb=None,
        eh_menor_doze_bb=False,
        eh_menor_idade_bb=True,
    )
    for cpf in menores_de_idade
]

rows += [
    Row(
        cpf_cnpj=cpf,
        nome_completo=None,
        sexo_bb=None,
        data_nascimento_bb=None,
        eh_menor_doze_bb=True,
        eh_menor_idade_bb=True,
    )
    for cpf in menores_de_doze
]

processed_df = spark.createDataFrame(rows, schema=schema)

# processed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar no s3 e no deltalake

# COMMAND ----------

if processed_df.count() > 0:
    processed_df.write.mode("append").option("overwriteSchema", "true").parquet(
        s3_file_path
    )

# TODO: criar função para remover um cpf específico da base já processada

# COMMAND ----------

# MAGIC %md
# MAGIC ### testa se deu certo

# COMMAND ----------

spark.read.parquet(s3_file_path).count()
