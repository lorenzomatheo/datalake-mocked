# Databricks notebook source
# MAGIC %pip install python-slugify
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Ambiente

# COMMAND ----------

import datetime as dt
import json

import boto3
import gspread
import pyspark.sql.functions as F
import pyspark.sql.types as T
from botocore.exceptions import NoCredentialsError
from google.oauth2.service_account import Credentials
from slugify import slugify

from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas.intercambiaveis import schema
from maggulake.utils.time import agora_em_sao_paulo_str

env = DatabricksEnvironmentBuilder.build(
    "process_intercambiaveis_anvisa",
    dbutils,
)

stage = env.settings.name_short
catalog = env.settings.catalog

print(f"Stage selecionado: {stage}")
print(f"Catalog selecionado: {catalog}")

# COMMAND ----------

env.create_table_if_not_exists(Table.intercambiaveis, schema)

# COMMAND ----------

# MAGIC %md # Config spark

# COMMAND ----------

spark = env.spark

# COMMAND ----------

# MAGIC %md ## Google Sheets Configuration

# COMMAND ----------

json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

gc = gspread.authorize(creds)

# planilha de correções manuais feita pelo time de QA (https://docs.google.com/spreadsheets/d/SHEET_ID)
SHEET_ID = "1oI8hHKXvNNwV2PtAUMCf8V2Jwd0H-LduJ3mVRTFxc7U"
ABA = "produtos_intercambiaveis"

rows = gc.open_by_key(SHEET_ID).worksheet(ABA).get_all_records()

df_sheets = spark.createDataFrame(rows)

# COMMAND ----------

# MAGIC %md
# MAGIC # Contagem de produtos antes da atualização
# MAGIC

# COMMAND ----------

# Conta o numero de produtos registrados na tabela "intercambiaveis" do databricks antes de atualizarmos os dados
intercambiaveis_atual = env.table(Table.intercambiaveis)

intercambiaveis_atual_count = intercambiaveis_atual.count()

print(
    "Quantidade de produtos registrados na tabela de intgercambiaveis antes da atualização: ",
    intercambiaveis_atual_count,
)


# COMMAND ----------

# MAGIC %md # Parametros do notebook

# COMMAND ----------

s3 = boto3.client("s3")

bucket_name = f"maggu-datalake-{stage}"
prefix = "1-raw-layer/maggu/consultas.anvisa.gov.br/"

# Lista todas as subpastas dentro de 's3_folder_medicamentos_anvisa'
try:
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    folders = [
        content["Prefix"].split("/")[-2]
        for content in response.get("CommonPrefixes", [])
    ]

    # Filtra as subpastas que representam datas e pega a mais recente
    date_folders = [folder for folder in folders if folder.isdigit()]
    data_mais_recente = max(date_folders, default="")

    data_mais_recente_datetime = dt.datetime.strptime(data_mais_recente, "%Y%m%d%H%M")

    s3_folder_medicamentos_anvisa = f"s3n://maggu-datalake-{stage}/1-raw-layer/maggu/consultas.anvisa.gov.br/{data_mais_recente}/medicamentos/"
    print(f"Utilizando dados do import mais recente: {data_mais_recente_datetime} UTC")
    print(f"Verifique o diretorio: {s3_folder_medicamentos_anvisa}")

except NoCredentialsError:
    print(
        "Houve algum erro com as credenciais AWS, verique a criação do seu cluster no databricks"
    )

# COMMAND ----------

# MAGIC %md # Leitura da camada raw

# COMMAND ----------

medicamentos = (
    spark.read.json(s3_folder_medicamentos_anvisa)
    .dropna(subset="medicamentoReferencia")
    .filter("categoriaRegulatoria in ('Similar', 'Genérico')")
    .filter("medicamentoReferencia != nomeComercial")
    .withColumn("numero_registro", F.col("numeroRegistro").cast("string"))
    .replace(
        {
            "Similar": TipoMedicamento.SIMILAR_INTERCAMBIAVEL.value,
            "Genérico": TipoMedicamento.GENERICO.value,
        },
        subset=["categoriaRegulatoria"],
    )
    .cache()
)

# COMMAND ----------

medicamentos_subset = medicamentos.select(
    F.lower(F.trim("principioAtivo")).alias("principioAtivo"),
    F.lower(F.trim("medicamentoReferencia")).alias("medicamentoReferencia"),
    F.lower(F.trim("nomeComercial")).alias("nomeComercial"),
    "categoriaRegulatoria",
    "numero_registro",
)

# COMMAND ----------


# Checkado manualmente essa extração do fabricante está safe
def extrair_fabricante(nome):
    if "merck" in nome:
        return "merck"
    if "ems" in nome:
        return "ems"
    if "novartis" in nome:
        return "novartis"
    if "sandoz" in nome:
        return "sandoz"
    return None


fabricante_udf = F.udf(lambda x: extrair_fabricante(x), T.StringType())
medicamentos_com_fabricante = medicamentos_subset.withColumn(
    "fabricante", fabricante_udf(F.col("medicamentoReferencia"))
)


# COMMAND ----------

guia_equivalentes_folder = env.full_s3_path(
    "1-raw-layer/maggu/guiadafarmacia_intercambialidade/"
)

guia_equivalentes_df = (
    spark.read.parquet(guia_equivalentes_folder)
    .selectExpr(
        "lower(trim(principio_ativo)) AS principioAtivo",
        'lower(trim(referencia)) AS medicamentoReferencia',
        'lower(trim(similar_equivalente)) AS nomeComercial',
        f"'{TipoMedicamento.SIMILAR_INTERCAMBIAVEL.value}' AS categoriaRegulatoria",
        "lower(trim(laboratorio)) AS fabricante",
        "null AS numero_registro",
    )
    .distinct()
)

# COMMAND ----------

subset = [
    "principioAtivo",
    "medicamentoReferencia",
    "nomeComercial",
    "categoriaRegulatoria",
    "numero_registro",
]

medicamentos_juntos = (
    medicamentos_com_fabricante.unionByName(
        guia_equivalentes_df, allowMissingColumns=True
    )
    .drop_duplicates(subset=subset)
    .cache()
)

# COMMAND ----------

medicamentos_filtrados = medicamentos_juntos.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(
        F.col("medicamentoReferencia"), "genérico da hypofarma", "hiperbarica hypofarma"
    ),
)

# remove todo texto entre parênteses
# pylint: disable=anomalous-backslash-in-string
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "\(.*\)", ""),
)

# remove previamento alguns textos
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(
        F.col("medicamentoReferencia"),
        "((medicamento)|(n/a)|(referência)|(similar)).*$",
        "",
    ),
)

# Um intercambiável pode ter vários referências separados por `;`
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia", F.explode(F.split("medicamentoReferencia", "; "))
)

# ... outro caractere separador de vários referências é o `/`, porém, é necessário limpar alguns textos antes de realizar o "split->explode"
# pylint: disable=anomalous-backslash-in-string
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(
        F.col("medicamentoReferencia"), "(\d.*mg/ml)|(\d.*mg/g)|(s/a)", ""
    ),
)
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "im/iv", "im-iv"),
)
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(
        F.col("medicamentoReferencia"),
        "advil -  dalsy -  alivium",
        "advil / dalsy / alivium",
    ),
)
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia", F.explode(F.split("medicamentoReferencia", "/"))
)

# ... outro caractere separador de vários referências é o ` e `, porém, é necessário limpar alguns textos antes de realizar o "split->explode"
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "creme e comprimidos", ""),
)
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia", F.explode(F.split("medicamentoReferencia", " e "))
)

# Realiza mais limpeza de texto
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "- novartis", ""),
)
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "- novartis", ""),
)
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia", F.regexp_replace(F.col("medicamentoReferencia"), "®$", "")
)
# medicamentos_filtrados = medicamentos_filtrados.withColumn('medicamentoReferencia', F.regexp_replace(F.col('medicamentoReferencia'), '((m\.s)|[&,]|(merck)|(novartis)|(acetelion)|(sanofi)|(genérico)|(eurofarma)|(bayer)).*$', ''))
# pylint: disable=anomalous-backslash-in-string
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "((m\.s)|[&,]|(genérico)).*$", ""),
)
# pylint: disable=anomalous-backslash-in-string
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia",
    F.regexp_replace(F.col("medicamentoReferencia"), "\d.*g", ""),
)

# Depois do Split, alguns textos ficarão com espaço sobrando no começo/final
medicamentos_filtrados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia", F.trim(F.col("medicamentoReferencia"))
)

medicamentos_filtrados = medicamentos_filtrados.filter(
    (medicamentos_filtrados.medicamentoReferencia != "")
).drop_duplicates(subset=subset)

# COMMAND ----------

# MAGIC %md Utiliza-se slugs para facilitar na comparação de strings dos medicamentos.

# COMMAND ----------

slugify_udf = F.udf(lambda x: slugify(x) if x is not None else None, T.StringType())

medicamentos_normalizados = medicamentos_filtrados.withColumn(
    "medicamentoReferencia_slug", slugify_udf(F.col("medicamentoReferencia"))
)
medicamentos_normalizados = medicamentos_normalizados.withColumn(
    "nomeComercial_slug", slugify_udf(F.col("nomeComercial"))
).cache()

# COMMAND ----------

# MAGIC %md # Cria uma lista de slugs de medicamentos

# COMMAND ----------

medicamentos_slugs_A = medicamentos_normalizados.selectExpr(
    "medicamentoReferencia_slug AS nome_slug",
    "principioAtivo",
    "fabricante",
    f"'{TipoMedicamento.REFERENCIA.value}' AS tipo_medicamento",
    "null AS numero_registro",
)

medicamentos_slugs_B = medicamentos_normalizados.selectExpr(
    "nomeComercial_slug AS nome_slug",
    "principioAtivo",
    "categoriaRegulatoria AS tipo_medicamento",
    "null::string As fabricante",
    "numero_registro",
)

medicamentos_slugs = medicamentos_slugs_A.unionByName(
    medicamentos_slugs_B
).drop_duplicates(["nome_slug", "principioAtivo", "fabricante"])
medicamentos_slugs = medicamentos_slugs.withColumn(
    "nome_slug_length", F.length(F.col("nome_slug"))
).cache()


# COMMAND ----------

# MAGIC %md # Leitura de produtos da conta Maggu para buscar EANs

# COMMAND ----------

produtos_maggu = env.table(Table.produtos_refined)

# COMMAND ----------

registro_col = (
    "numero_registro" if "numero_registro" in produtos_maggu.columns else "registro"
)

produtos_maggu_filtrado = (
    produtos_maggu.filter(produtos_maggu.eh_medicamento == True)
    .dropna(subset=["marca", "principio_ativo"])
    .select(
        F.col("ean"),
        F.col("marca"),
        F.col("principio_ativo"),
        F.col(registro_col).alias("numero_registro"),
    )
    .withColumn("principio_ativo", F.trim(F.lower(F.col("principio_ativo"))))
    .withColumn("marca", F.trim(F.lower(F.col("marca"))))
    .withColumn("marca_slug", slugify_udf(F.col("marca")))
).cache()


# COMMAND ----------

# MAGIC %md # Processamento

# COMMAND ----------

# MAGIC %md ### Join por registro ANVISA (mais confiável)

# COMMAND ----------

# Join por registro - captura matches exatos quando ambos têm registro
medicamentos_slugs_registro_join = (
    medicamentos_slugs.alias("s")
    .join(
        F.broadcast(produtos_maggu_filtrado).alias("p"),
        (F.col("s.numero_registro").isNotNull())
        & (F.col("p.numero_registro").isNotNull())
        & (F.col("s.numero_registro") == F.col("p.numero_registro")),
    )
    .select(
        F.col("s.nome_slug"),
        F.col("s.principioAtivo"),
        F.col("s.tipo_medicamento"),
        F.col("s.fabricante"),
        F.col("s.numero_registro"),
        F.col("s.nome_slug_length"),
        F.col("p.ean"),
        F.col("p.marca"),
        F.col("p.principio_ativo"),
        F.col("p.marca_slug"),
    )
)

# Medicamentos que não tiveram match por registro (fallback para slug)
medicamentos_slugs_sem_registro = medicamentos_slugs.alias("s").join(
    medicamentos_slugs_registro_join.select("nome_slug", "principioAtivo", "fabricante")
    .distinct()
    .alias("matched"),
    (F.col("s.nome_slug") == F.col("matched.nome_slug"))
    & (F.col("s.principioAtivo") == F.col("matched.principioAtivo"))
    & (
        F.coalesce(F.col("s.fabricante"), F.lit(""))
        == F.coalesce(F.col("matched.fabricante"), F.lit(""))
    ),
    "leftanti",
)

# COMMAND ----------

# MAGIC %md ### Slug idêntico (fallback para produtos sem registro)

# COMMAND ----------

medicamentos_slugs_join = (
    medicamentos_slugs_sem_registro.alias("s")
    .join(
        F.broadcast(produtos_maggu_filtrado).alias("p"),
        F.col("p.marca_slug") == F.col("s.nome_slug"),
    )
    .select(
        F.col("s.nome_slug"),
        F.col("s.principioAtivo"),
        F.col("s.tipo_medicamento"),
        F.col("s.fabricante"),
        F.col("s.numero_registro"),
        F.col("s.nome_slug_length"),
        F.col("p.ean"),
        F.col("p.marca"),
        F.col("p.principio_ativo"),
        F.col("p.marca_slug"),
    )
)

# Unir matches por registro com matches por slug
medicamentos_slugs_join = medicamentos_slugs_registro_join.unionByName(
    medicamentos_slugs_join
)


# COMMAND ----------

# MAGIC %md ### Slug com fabricante
# MAGIC
# MAGIC Alguns medicamentos Similares possuem um genérico como referência. O nome deve conter o fabricante para conseguir fazer o match

# COMMAND ----------

medicamentos_slugs_not_found = medicamentos_slugs.alias("s").join(
    medicamentos_slugs_join.alias("e"),
    F.col("e.nome_slug") == F.col("s.nome_slug"),
    "leftanti",
)
medicamentos_slugs_not_found = medicamentos_slugs_not_found.filter(
    medicamentos_slugs_not_found.fabricante.isNotNull()
)

medicamentos_slugs_fabricante = medicamentos_slugs_not_found.withColumn(
    "nome_slug_fabricante", F.concat_ws("-", F.col("nome_slug"), F.col("fabricante"))
)

medicamentos_slugs_fabricante_join = (
    medicamentos_slugs_fabricante.alias("s")
    .join(
        F.broadcast(produtos_maggu_filtrado).alias("p"),
        F.col("p.marca_slug") == F.col("s.nome_slug_fabricante"),
    )
    .select(
        F.col("s.nome_slug"),
        F.col("s.principioAtivo"),
        F.col("s.tipo_medicamento"),
        F.col("s.fabricante"),
        F.col("s.numero_registro"),
        F.col("s.nome_slug_length"),
        F.col("p.ean"),
        F.col("p.marca"),
        F.col("p.principio_ativo"),
        F.col("p.marca_slug"),
    )
)

# COMMAND ----------

slugs_to_medicamentos_maggu = medicamentos_slugs_join.unionByName(
    medicamentos_slugs_fabricante_join
).cache()

# COMMAND ----------

# MAGIC %md ### Slug - início de string
# MAGIC
# MAGIC Isso resolve quando
# MAGIC - há fabricante no final. ex: `dipirona` <> `dipirona-ems`, `acr` <> `acr-merck`
# MAGIC - há característica no nome. ex: `dipirona` <> `dipirona-gotas`, `acr` <> `acr-geléia`

# COMMAND ----------

medicamentos_slugs_not_found = medicamentos_slugs.alias("s").join(
    F.broadcast(slugs_to_medicamentos_maggu).alias("e"),
    F.col("e.nome_slug") == F.col("s.nome_slug"),
    "leftanti",
)

traco_udf = F.udf(lambda x: x + "-", T.StringType())
medicamentos_slugs_not_found_traco = medicamentos_slugs_not_found.withColumn(
    "nome_slug_traco", traco_udf(F.col("nome_slug"))
).filter(medicamentos_slugs_not_found.nome_slug_length > 5)
medicamentos_slugs_traco_join = (
    medicamentos_slugs_not_found_traco.alias("s")
    .join(
        F.broadcast(produtos_maggu_filtrado).alias("p"),
        F.col("p.marca_slug").startswith(F.col("s.nome_slug_traco")),
    )
    .select(
        F.col("s.nome_slug"),
        F.col("s.principioAtivo"),
        F.col("s.tipo_medicamento"),
        F.col("s.fabricante"),
        F.col("s.numero_registro"),
        F.col("s.nome_slug_length"),
        F.col("p.ean"),
        F.col("p.marca"),
        F.col("p.principio_ativo"),
        F.col("p.marca_slug"),
    )
)

# COMMAND ----------

slugs_to_medicamentos_maggu = slugs_to_medicamentos_maggu.unionByName(
    medicamentos_slugs_traco_join
).cache()

# COMMAND ----------

# MAGIC %md ### Slug - início de string - alguns ultimos caracteres
# MAGIC
# MAGIC Isso resolve quando
# MAGIC - há um sufixo . ex: `dipirona-xy` <> `dipirona`, `acr-ab` <> `acr`

# COMMAND ----------

medicamentos_slugs_not_found = medicamentos_slugs.alias("s").join(
    F.broadcast(slugs_to_medicamentos_maggu).alias("e"),
    F.col("e.nome_slug") == F.col("s.nome_slug"),
    "leftanti",
)

medicamentos_slugs_not_found_trim = (
    medicamentos_slugs_not_found.withColumn(
        "nome_slug_trim",
        F.regexp_replace(
            F.col("nome_slug"), "-((...)|(..)|(.)|(.-.)|(..-.)|(.-..))$", ""
        ),
    )
    .withColumn("nome_slug_trim", traco_udf(F.col("nome_slug_trim")))
    .filter(medicamentos_slugs_not_found.nome_slug_length > 5)
)
medicamentos_slugs_trim_join = (
    medicamentos_slugs_not_found_trim.alias("s")
    .join(
        produtos_maggu_filtrado.alias("p"),
        F.col("p.marca_slug").startswith(F.col("s.nome_slug_trim")),
    )
    .select(
        F.col("s.nome_slug"),
        F.col("s.principioAtivo"),
        F.col("s.tipo_medicamento"),
        F.col("s.fabricante"),
        F.col("s.numero_registro"),
        F.col("s.nome_slug_length"),
        F.col("p.ean"),
        F.col("p.marca"),
        F.col("p.principio_ativo"),
        F.col("p.marca_slug"),
    )
)

# COMMAND ----------

slugs_to_medicamentos_maggu = slugs_to_medicamentos_maggu.unionByName(
    medicamentos_slugs_trim_join
)

# COMMAND ----------

# MAGIC %md ### Slug - levenshtein
# MAGIC
# MAGIC Alguns medicamentos estão nomes mal redigidos

# COMMAND ----------

medicamentos_slugs_not_found = medicamentos_slugs.alias("s").join(
    slugs_to_medicamentos_maggu.alias("e"),
    F.col("e.nome_slug") == F.col("s.nome_slug"),
    "leftanti",
)

medicamentos_slugs_intercambiaveis = (
    medicamentos_slugs_not_found.alias("s")
    .filter(F.col("nome_slug_length") > 5)
    .join(
        produtos_maggu_filtrado.alias("p"),
        F.levenshtein(F.col("p.marca_slug"), F.col("s.nome_slug"))
        < 2,  # alguns medicamentos podem possuir pequenos erros nos nomes
    )
)
medicamentos_slugs_intercambiaveis = medicamentos_slugs_intercambiaveis.withColumn(
    "p_a_1", slugify_udf(F.col("principioAtivo"))
).withColumn("p_a_2", slugify_udf(F.col("principio_ativo")))
medicamentos_slugs_intercambiaveis = medicamentos_slugs_intercambiaveis.filter(
    F.levenshtein(F.col("p_a_1"), F.col("p_a_2")) < 2
).select(
    F.col("s.nome_slug"),
    F.col("s.principioAtivo"),
    F.col("s.tipo_medicamento"),
    F.col("s.fabricante"),
    F.col("s.numero_registro"),
    F.col("s.nome_slug_length"),
    F.col("p.ean"),
    F.col("p.marca"),
    F.col("p.principio_ativo"),
    F.col("p.marca_slug"),
)

# COMMAND ----------

slugs_to_medicamentos_maggu = slugs_to_medicamentos_maggu.unionByName(
    medicamentos_slugs_intercambiaveis
).cache()

# COMMAND ----------


# COMMAND ----------

# MAGIC %md # Cria tabela de intercambiáveis

# COMMAND ----------


# COMMAND ----------

slugs_referencias = slugs_to_medicamentos_maggu.filter(
    F.col("tipo_medicamento") == TipoMedicamento.REFERENCIA
)
slugs_intercambiaveis = slugs_to_medicamentos_maggu.filter(
    F.col("tipo_medicamento") != TipoMedicamento.REFERENCIA
)

# COMMAND ----------

# Join dos intercambiáveis: preferência por registro, fallback para slug
intercambiaveis_processados = (
    medicamentos_normalizados.alias("m")
    .join(
        F.broadcast(slugs_referencias).alias("pr"),
        F.col("pr.nome_slug") == F.col("m.medicamentoReferencia_slug"),
    )
    .join(
        F.broadcast(slugs_intercambiaveis).alias("ps"),
        # Match por registro quando ambos têm valor
        (
            (F.col("m.numero_registro").isNotNull())
            & (F.col("ps.numero_registro").isNotNull())
            & (F.col("m.numero_registro") == F.col("ps.numero_registro"))
        )
        # Fallback por slug APENAS quando não é possível comparar por registro
        | (
            (F.col("m.numero_registro").isNull() | F.col("ps.numero_registro").isNull())
            & (F.col("ps.nome_slug") == F.col("m.nomeComercial_slug"))
        ),
    )
    .selectExpr(
        "pr.ean AS ean_referencia",
        "pr.marca AS marca_referencia",
        "pr.principio_ativo AS principio_ativo",
        "ps.ean AS ean_intercambiavel",
        "ps.marca AS marca_intercambiavel",
        "ps.tipo_medicamento AS tipo_intercambiavel",
        "m.numero_registro AS numero_registro_intercambiavel",
    )
    .withColumn("arquivo_s3", F.lit(s3_folder_medicamentos_anvisa))
    .withColumn("gerado_em", F.lit(dt.datetime.now()))
).cache()

# COMMAND ----------

intercambiaveis_processados = intercambiaveis_processados.drop_duplicates()

# COMMAND ----------

# MAGIC %md # Remove produtos intercambiaveis que estão na planilha do Google Sheets como INCORRETO

# COMMAND ----------

incorretos_sheets = (
    df_sheets.filter(F.col("avaliacao") == "INCORRETO")
    .select(F.col("ean_referencia"), F.col("ean_similar").alias("ean_intercambiavel"))
    .collect()
)

combinacoes_eans_para_excluir = [
    (row["ean_referencia"], row["ean_intercambiavel"]) for row in incorretos_sheets
]

print(f"Serão removidos {len(combinacoes_eans_para_excluir)} pares de eans")

if combinacoes_eans_para_excluir:
    exclude_df = spark.createDataFrame(
        combinacoes_eans_para_excluir,
        ["ean_referencia_excluir", "ean_intercambiavel_excluir"],
    )

    intercambiaveis_processados = (
        intercambiaveis_processados.alias("i")
        .join(
            F.broadcast(exclude_df).alias("e"),
            (F.col("i.ean_referencia") == F.col("e.ean_referencia_excluir"))
            & (F.col("i.ean_intercambiavel") == F.col("e.ean_intercambiavel_excluir")),
            "leftanti",
        )
        .select("i.*")
        .cache()
    )
else:
    print("Nenhum produto encontrado para remover")

# COMMAND ----------

# tempo para contabilizar o 'intercambiaveis_processados'

intercambiaveis_processados.count()

# COMMAND ----------

intercambiaveis_processados.write.mode('overwrite').option(
    "overwriteSchema", "true"
).saveAsTable(Table.intercambiaveis.value)
print("Salvo no databricks! Horário: ", agora_em_sao_paulo_str())
