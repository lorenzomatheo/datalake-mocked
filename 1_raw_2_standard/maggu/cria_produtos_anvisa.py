# Databricks notebook source
import datetime as dt

import boto3
import pyspark.sql.functions as F

from maggulake.enums.faixa_etaria import FaixaEtariaSimplificado
from maggulake.enums.medicamentos import TiposTarja
from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas.medicamentos_anvisa import schema

env = DatabricksEnvironmentBuilder.build("atualiza_produtos_anvisa", dbutils)

# COMMAND ----------

spark = env.spark
stage = env.settings.name_short
bucket_name = env.settings.bucket

# COMMAND ----------

s3 = boto3.client("s3")


prefix = "1-raw-layer/maggu/consultas.anvisa.gov.br/"

# Lista todas as subpastas dentro de 's3_folder_medicamentos_anvisa'
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
folders = [
    content["Prefix"].split("/")[-2] for content in response.get("CommonPrefixes", [])
]

# Filtra as subpastas que representam datas e pega a mais recente
date_folders = [folder for folder in folders if folder.isdigit()]
data_mais_recente = max(date_folders, default="")

data_mais_recente_datetime = dt.datetime.strptime(data_mais_recente, "%Y%m%d%H%M")

s3_folder_medicamentos_anvisa = env.full_s3_path(
    f"{prefix}/{data_mais_recente}/medicamentos/"
)
print(f"Utilizando dados do import mais recente: {data_mais_recente_datetime} UTC")
print(f"Verifique o diretorio: {s3_folder_medicamentos_anvisa}")

# COMMAND ----------

medicamentos_raw = spark.read.json(s3_folder_medicamentos_anvisa).cache()

# COMMAND ----------

medicamentos = (
    medicamentos_raw.selectExpr(
        "numeroRegistro AS numero_registro",
        "nomeComercial AS marca",
        "empresa.razaoSocial AS fabricante",
        "apresentacoes[0].restricaoUso[0] AS idade_recomendada",
        "categoriaRegulatoria AS tipo_medicamento",
        "medicamentoReferencia",
        "apresentacoes[0].tarja AS tarja",
        "apresentacoes[0].restricaoPrescricao[0] AS tipo_prescricao",
        "initcap(apresentacoes[0].viasAdministracao[0]) AS via_administracao",
        "lower(apresentacoes[0].embalagemPrimariaTodas[0].tipo) AS forma_farmaceutica",
        "principioAtivo AS principio_ativo",
        "true AS eh_medicamento",
    )
    .withColumn(
        "tarja",
        F.when(F.col("tarja") == "Vermelha", TiposTarja.VERMELHA)
        .when(
            F.col("tarja") == "Vermelha sob restrição",
            TiposTarja.VERMELHA_SOB_RESTRICAO,
        )
        .when(F.col("tarja") == "Preta", TiposTarja.PRETA)
        .when(F.col("tarja") == "Sem Tarja", TiposTarja.SEM_TARJA),
    )
    .withColumn(
        "eh_medicamento",
        F.lit(True),
    )
    .withColumn(
        "eh_tarjado",
        F.startswith(F.col("tarja"), F.lit("Vermelha"))
        | (F.col("tarja") == TiposTarja.PRETA),
    )
    .withColumn(
        "eh_controlado",
        (F.col("tarja") == TiposTarja.VERMELHA_SOB_RESTRICAO)
        | (F.col("tarja") == TiposTarja.PRETA),
    )
    .withColumn(
        "eh_otc",
        (F.col("tarja") == TiposTarja.SEM_TARJA)
        | (F.col("tarja") == TiposTarja.VERMELHA),
    )
    .replace(
        {
            "Radiofármaco": "Medicamento Radiofármaco",
            "Biológico": "Medicamento Biológico",
            "Específico": "Medicamento Específico",
            "Novo": TipoMedicamento.REFERENCIA.value,
            "Dinamizado": "Medicamento Dinamizado",
            "Fitoterápico": "Medicamento Fitoterápico",
            "Genérico": TipoMedicamento.GENERICO.value,
        },
        subset=["tipo_medicamento"],
    )
    .withColumn(
        "tipo_medicamento",
        F.when(
            (F.col("tipo_medicamento") == "Similar")
            & F.col("medicamentoReferencia").isNotNull(),
            F.lit(TipoMedicamento.SIMILAR_INTERCAMBIAVEL.value),
        )
        .when(
            F.col("tipo_medicamento") == "Similar",
            F.lit(TipoMedicamento.SIMILAR.value),
        )
        .otherwise(F.col("tipo_medicamento")),
    )
    .drop("medicamentoReferencia")
    .withColumn(
        "idade_recomendada",
        F.when(
            F.startswith("idade_recomendada", F.lit("Adulto e Pediátrico")),
            FaixaEtariaSimplificado.TODOS,
        )
        .when(
            F.startswith("idade_recomendada", F.lit("Adulto")),
            FaixaEtariaSimplificado.ADULTO,
        )
        .when(
            F.startswith("idade_recomendada", F.lit("Pediátrico")),
            FaixaEtariaSimplificado.CRIANCA,
        )
        .otherwise(F.col("idade_recomendada")),
    )
    .drop("tipo_prescricao")
    .cache()
)

# COMMAND ----------

env.create_table_if_not_exists(Table.medicamentos_anvisa, schema)

# COMMAND ----------

medicamentos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    Table.medicamentos_anvisa.value
)
