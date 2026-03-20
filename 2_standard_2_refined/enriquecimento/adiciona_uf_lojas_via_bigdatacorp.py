# Databricks notebook source
get_ipython().system("pip install aiohttp --upgrade")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Enriquece lojas com dados do bigboost

# COMMAND ----------

import asyncio
import json
from datetime import datetime

import aiohttp
import pyspark.sql.functions as F
import pyspark.sql.types as T
import requests

from maggulake.environment.environment import DatabricksEnvironmentBuilder
from maggulake.utils.iters import create_batches, limita_iterable_a_x_elementos

# COMMAND ----------


# Configurar ambiente - padrão para produção
# NOTE: Este notebook precisa rodar em produção por padrão
env = DatabricksEnvironmentBuilder.build(
    "adiciona_uf_lojas_via_bigdatacorp",
    dbutils,
    widgets={"environment": ["production", "staging", "production"]},
)

# COMMAND ----------

spark = env.spark
postgres = env.postgres_adapter
bigdatacorp_client = env.bigdatacorp_client

# URL para busca de empresas
URL = bigdatacorp_client.address + "empresas"
HEADERS = bigdatacorp_client.headers
QTD_LIMITE_DE_BUSCAS = 1000
cnpj_lojas_processados_s3_file_path = env.full_s3_path(
    "3-refined-layer/cnpj_lojas_processados"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega lojas sem estado

# COMMAND ----------

df_lojas = (
    postgres.read_table(spark, table="contas_loja")
    .filter(
        F.col('cnpj').isNotNull()
        & (F.trim(F.col('cnpj')) != '')
        & (F.trim(F.col('cnpj')) != '00000000000000')
    )
    .filter(
        (F.col('estado').isNull() | (F.trim(F.col('estado')) == ''))
        | (F.length(F.col('estado')) > 2)
    )
    .select(
        'id',
        'cnpj',
        'estado',
    )
)

df_lojas = df_lojas.dropDuplicates(["cnpj"])

# COMMAND ----------

print("Quantidade de lojas sem UF: ", df_lojas.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar DataFrame de CNPJs já processados

# COMMAND ----------

already_processed_df = (
    spark.read.parquet(cnpj_lojas_processados_s3_file_path).select("cnpj").distinct()
)

# Realizar o anti join para obter apenas os CNPJs que não foram processados
a_fazer_df = df_lojas.join(already_processed_df, on=["cnpj"], how="leftanti")

print("Quantidade de itens já processados: ", already_processed_df.count())

# COMMAND ----------

cnpj_list = [row["cnpj"] for row in a_fazer_df.select("cnpj").collect()]

print("Quantidade de CNPJs a processar: ", len(cnpj_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função assíncrona em batches

# COMMAND ----------


async def get_data_from_api_async(cnpj, session):
    payload = {"Datasets": "basic_data", "q": f"doc{{{cnpj}}}"}
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
                        print(f"Erro na resposta para cnpj {cnpj}: {result}")
                        errors_dict[cnpj] = result
                        return None

                    if int(status) != 0:
                        print(f"Erro na resposta para cnpj {cnpj}: {result}")
                        errors_dict[cnpj] = result
                        return None

                    data = result.get("Result", [{}])[0].get("BasicData")
                    if not data:
                        print("\t Data não encontrado - cnpj: ", cnpj, result)
                        errors_dict[cnpj] = result
                        return None

                    print("cnpj concluído com sucesso! ", cnpj)
                    return {
                        'estado': data.get('HeadquarterState'),
                        'buscado_em': datetime.now(),
                        'payload_completo': json.dumps(result, ensure_ascii=False),
                    }
                else:
                    print(f"Erro na requisição para cnpj {cnpj}: {response.status}")
                    errors_dict[cnpj] = response.text
        except requests.RequestException as e:
            print(f"Erro na tentativa {attempt + 1} para cnpj {cnpj}: {e}")
            errors_dict[cnpj] = e
            await asyncio.sleep(2**attempt)  # Exponential backoff
    return None


async def process_batch(batch, session):
    tasks = [get_data_from_api_async(cnpj, session) for cnpj in batch]
    results = await asyncio.gather(*tasks)
    return results


async def main(cnpj_list):
    async with aiohttp.ClientSession() as session:
        processed_count = 0
        for batch in create_batches(cnpj_list, batch_size):
            results = await process_batch(batch, session)

            # processar resultados do batch
            for cnpj, result_data in zip(batch, results):
                if result_data is not None:
                    results_dict[cnpj] = result_data
                else:
                    errors_list.append(cnpj)

            processed_count += len(batch)
            print(f"Processados {processed_count} de {n_cnpjs}")
            await asyncio.sleep(5)  # evitando rate limit


# COMMAND ----------

# Executar o processamento assíncrono
batch_size = 100
max_retries = 2
errors_list = []
errors_dict = {}
results_dict = {}

n_cnpjs = len(cnpj_list)

if n_cnpjs == 0:
    dbutils.notebook.exit("Nenhum CNPJ encontrado para processar. Saindo do pipeline")

# limitar para não explodir o custo
cnpj_list = limita_iterable_a_x_elementos(cnpj_list, QTD_LIMITE_DE_BUSCAS)

await main(cnpj_list)

# COMMAND ----------

print("quantidade de CNPJs que não puderam ser preenchidos: ", len(errors_list))
print("quantidade de CNPJs preenchidos: ", len(results_dict))

success_rate = len(results_dict) / n_cnpjs * 100
print(f"taxa de sucesso: {success_rate:.2f} %")

# COMMAND ----------

schema = T.StructType(
    [
        T.StructField("cnpj", T.StringType(), True),
        T.StructField("estado", T.StringType(), True),
        T.StructField("buscado_em", T.TimestampType(), True),
        T.StructField("payload_completo", T.StringType(), True),
    ]
)

processed_data = [
    (cnpj, data['estado'], data['buscado_em'], data['payload_completo'])
    for cnpj, data in results_dict.items()
]

processed_df = spark.createDataFrame(processed_data, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar no s3 e no postgres

# COMMAND ----------


def inserir_estado_nas_lojas_postgres(processed_df, postgres):
    rows = processed_df.collect()
    cnpj_list = [row.cnpj for row in rows]
    estado_list = [row.estado for row in rows]

    if not cnpj_list:
        return

    placeholders = ','.join(['%s'] * len(cnpj_list))
    case_when_parts = []
    params = []

    for cnpj, estado in zip(cnpj_list, estado_list):
        case_when_parts.append("WHEN %s THEN %s")
        params.extend([cnpj, estado])

    params.extend(cnpj_list)

    update_query = f"""
        UPDATE contas_loja
        SET estado = CASE cnpj
            {' '.join(case_when_parts)}
            ELSE estado
        END
        WHERE cnpj IN ({placeholders})
    """

    postgres.execute_query_with_params(update_query, params)


# COMMAND ----------

if processed_df.count() > 0:
    processed_df.write.mode("append").option("overwriteSchema", "true").parquet(
        cnpj_lojas_processados_s3_file_path
    )

    inserir_estado_nas_lojas_postgres(processed_df, postgres)

    print(f"{processed_df.count()} lojas atualizadas com sucesso!")
    processed_df.display()
else:
    print("Nenhuma loja foi atualizada nessa execução!")
