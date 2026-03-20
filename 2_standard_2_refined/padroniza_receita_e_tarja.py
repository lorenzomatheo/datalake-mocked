# Databricks notebook source
# MAGIC %pip install pandas fsspec s3fs openpyxl xlrd
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import warnings

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Row

from maggulake.enums import DescricaoTipoReceita, TiposReceita
from maggulake.environment import DatabricksEnvironmentBuilder, Table

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build("padroniza_receita_e_tarja", dbutils)

# COMMAND ----------

# spark
spark = env.spark

# ambiente e cliente
stage = env.settings.stage
catalog = env.settings.catalog
s3_bucket_name = f"maggu-datalake-{stage}"
DEBUG = dbutils.widgets.get("debug") == "true"

# pastas s3
s3_folder_anvisa = env.full_s3_path(
    "1-raw-layer/maggu/consultas.anvisa.gov.br/info-medicamentos-e-otcs/"
)
s3_file_medicamentos = s3_folder_anvisa + "xls_conformidade_site_20240429_164029250.xls"


# COMMAND ----------

produtos = env.table(Table.produtos_em_processamento)
produtos = produtos.drop('validade_receita_dias')


# COMMAND ----------

if DEBUG:
    produtos.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Corrigindo texto de receita e incluindo info de tarja e validade

# COMMAND ----------

# TODO: mapeamento gigante hardcoded de receita/tarja. Extrair para config externa ou tabela.
tipo_de_receita_mapping = {
    "A1 Amarela (Dispensação Sob Prescrição Médica Restrito a Hospitais - Este medicamento pode causar Dependência Física ou Psíquica)": (
        DescricaoTipoReceita.A1_AMARELA_HOSPITAL.value,
        "Preta",
        30,
    ),
    "A1 Amarela (Venda sob Prescrição Médica - Este medicamento pode causar Dependência Física ou Psíquica)": (
        DescricaoTipoReceita.A1_AMARELA.value,
        "Preta",
        30,
    ),
    "A3 Amarela (Venda sob Prescrição Médica - Este medicamento pode causar Dependência Física ou Psíquica)": (
        DescricaoTipoReceita.A3_AMARELA.value,
        "Preta",
        30,
    ),
    "B1 Azul (Venda sob Prescrição Médica - O Abuso deste Medicamento pode causar Dependência)": (
        DescricaoTipoReceita.B1_AZUL.value,
        "Preta",
        30,
    ),
    "B2 Azul (Venda sob Prescrição Médica - O Abuso deste Medicamento pode causar Dependência)": (
        DescricaoTipoReceita.B2_AZUL.value,
        "Preta",
        30,
    ),
    "Branca 2 Vias (Antibiótico - Dispensação Sob Prescrição Médica Restrito a Hospitais)": (
        DescricaoTipoReceita.BRANCA_2_VIAS_HOSPITAL.value,
        "Vermelha",
        10,
    ),
    "Branca 2 vias (Antibiótico - Venda Sob Prescrição Médica mediante Retenção da Receita)": (
        DescricaoTipoReceita.BRANCA_2_VIAS.value,
        "Vermelha",
        10,
    ),
    "C1 Branca 2 Vias (Dispensação Sob Prescrição Médica Restrito a Hospitais - Este medicamento pode causar Dependência Física ou Psíquica)": (
        DescricaoTipoReceita.C1_BRANCA_2_VIAS_HOSPITAL.value,
        "Vermelha",
        30,
    ),
    "C1 Branca 2 vias (Venda Sob Prescrição Médica - Este medicamento pode causar Dependência Física ou Psíquica)": (
        DescricaoTipoReceita.C1_BRANCA_2_VIAS.value,
        "Vermelha",
        30,
    ),
    "C2 Branca (Venda Sob Prescrição Médica - Não use este Medicamento sem Consultar o seu Médico, caso esteja Grávida. Ele pode causar Problemas ao Feto)": (
        DescricaoTipoReceita.C2_BRANCA.value,
        "Vermelha",
        30,
    ),
    "C3 Branca (Venda Sob Prescrição Médica - Não use este Medicamento sem Consultar o seu Médico, caso esteja Grávida. Ele pode causar Problemas ao Feto)": (
        DescricaoTipoReceita.C3_BRANCA.value,
        "Preta e Vermelha",
        20,
    ),
    "C5 Branca 2 vias (Venda Sob Prescrição Médica - Só Pode ser Vendido com Retenção da Receita)": (
        DescricaoTipoReceita.C5_BRANCA_2_VIAS.value,
        "Vermelha",
        30,
    ),
    "(inativado) não usar - branca 2 vias (venda sob prescrição médica)": (
        DescricaoTipoReceita.BRANCA_2_VIAS.value,
        "",
        None,
    ),
    "branca comum - vacinas (venda sob prescrição médica)": (
        DescricaoTipoReceita.BRANCA_COMUM_VACINAS.value,
        "",
        None,
    ),
    "não precisa de receita": (DescricaoTipoReceita.ISENTO.value, "", None),
    "receita simples - veterinário": (
        DescricaoTipoReceita.RECEITUARIO_SIMPLES.value,
        "",
        None,
    ),
    "receita branca para importação": (
        DescricaoTipoReceita.BRANCA_COMUM.value,
        "",
        None,
    ),
    " Branca Comum": (DescricaoTipoReceita.BRANCA_COMUM.value, "", None),
    "não informado": (DescricaoTipoReceita.DESCONHECIDO.value, "", None),
}

mapping_df = spark.createDataFrame(
    [(key, *value) for key, value in tipo_de_receita_mapping.items()],
    [
        "tipo_de_receita_completo",
        "TEXTO_CORRETO",
        "tarja_inferred",
        "validade_receita_dias",
    ],
)

final_df = produtos.withColumn(
    "tipo_de_receita_completo", F.trim(F.lower(F.col("tipo_de_receita_completo")))
)
mapping_df = mapping_df.withColumn(
    "tipo_de_receita_completo", F.trim(F.lower(F.col("tipo_de_receita_completo")))
)


final_df = final_df.join(mapping_df, "tipo_de_receita_completo", "left")

final_df = (
    final_df.withColumn(
        "tipo_de_receita_completo",
        F.lower(F.coalesce("TEXTO_CORRETO", "tipo_de_receita_completo")),
    )
    .withColumn("tarja", F.coalesce("tarja", "tarja_inferred"))
    .withColumn(
        "validade_receita_dias", F.coalesce("validade_receita_dias", F.lit(None))
    )
)

final_df = final_df.drop("TEXTO_CORRETO", "tarja_inferred")

# COMMAND ----------

final_df = final_df.withColumn(
    "tipo_receita",
    F.lower(
        F.coalesce(
            F.split_part("tipo_de_receita_completo", F.lit(" ("), F.lit(1)),
            "tipo_receita",
        )
    ),
)

# transforma coluna "tipo_receita" em lower case
final_df = final_df.withColumn("tipo_receita", F.lower(F.col("tipo_receita")))

# COMMAND ----------

# padronizando os tipos de receita que faltavam
final_df = final_df.withColumn(
    "tipo_receita",
    F.when(F.col("tipo_receita") == "não precisa de receita", TiposReceita.ISENTO.value)
    .when(
        F.col("tipo_receita").startswith("só pode ser vendido com "),
        TiposReceita.RECEITUARIO_SIMPLES.value,
    )
    .when(
        F.col("tipo_receita").startswith("receita branca para importa"),
        TiposReceita.BRANCA_COMUM.value,
    )
    .when(
        F.col("tipo_receita") == "venda sob prescrição médica.",
        TiposReceita.RECEITUARIO_SIMPLES.value,
    )
    .when(
        F.col("tipo_receita") == "controle especial - veterinário",
        TiposReceita.CONTROLE_ESPECIAL.value,
    )
    .when(F.col("tipo_receita") == "não informado", TiposReceita.DESCONHECIDO.value)
    .when(
        F.col("tipo_receita") == "receita branca comum", TiposReceita.BRANCA_COMUM.value
    )
    .when(F.col("tipo_receita") == "não se aplica", TiposReceita.DESCONHECIDO.value)
    .otherwise(F.col("tipo_receita")),
)

# COMMAND ----------

# hardcode RESOLUÇÃO DA DIRETORIA COLEGIADA ANVISA Nº 877, DE 28 DE MAIO DE 2024 entra em vigor 1/ago/24
# altera eszopiclona, zopiclona e zolpidem para B1_AZUL

final_df = final_df.withColumn(
    "tipo_receita",
    F.when(
        F.lower(F.col("principio_ativo")).contains("zolpidem")
        | F.lower(F.col("principio_ativo")).contains("zopiclona"),
        TiposReceita.B1_AZUL.value,
    ).otherwise(F.col("tipo_receita")),
)
final_df = final_df.withColumn(
    "tipo_de_receita_completo",
    F.when(
        F.lower(F.col("principio_ativo")).contains("zolpidem")
        | F.lower(F.col("principio_ativo")).contains("zopiclona"),
        DescricaoTipoReceita.B1_AZUL.value,
    ).otherwise(F.col("tipo_de_receita_completo")),
)

# COMMAND ----------

final_df.select(["tipo_receita", "tipo_de_receita_completo"]).distinct().display()

# COMMAND ----------

# Se qualquer valor da coluna "tipo_receita" não estiver no enum TiposReceita, levanta um warning

opcoes = TiposReceita.list()


def validar_tipo_receita(row: Row) -> tuple[str, str | None] | None:
    if row.tipo_receita is None:
        return None
    if row.tipo_receita not in opcoes:
        return (row.ean, row.tipo_receita)
    return None


invalid_rows = (
    final_df.select("ean", "tipo_receita")
    .rdd.map(validar_tipo_receita)
    .filter(lambda x: x is not None)
    .collect()
)

for row in invalid_rows:
    warnings.warn(f"EAN {row[0]} tem tipo_receita inválido: {row[1]}", UserWarning)

# COMMAND ----------

# MAGIC %md
# MAGIC Pega info tarja da planilha de medicamentos da Anvisa

# COMMAND ----------

info_medicamentos = pd.read_excel(
    s3_file_medicamentos, sheet_name="Planilha1", skiprows=41, header=0
)
medicamentos_anvisa_df = spark.createDataFrame(info_medicamentos)

print(f"Total de linhas na tabela de medicamentos: {medicamentos_anvisa_df.count()}")
medicamentos_anvisa_df.display()

# COMMAND ----------

medicamentos_anvisa_df.select("TARJA").distinct().display()

# COMMAND ----------

final_df.limit(10).display()

# COMMAND ----------

final_df.select("tarja").distinct().display()
print(
    f"Produtos sem tarja vazia: {final_df.filter(F.col('tarja').isNotNull()).count()}"
)

# COMMAND ----------

tarja_vermelha = ["Tarja Vermelha", "Tarja Vermelha sob restrição"]
tarja_preta = ["Tarja Preta"]
sem_tarja = ["Tarja Sem Tarja"]

eans_tarja_vermelha = (
    medicamentos_anvisa_df.filter(F.col("TARJA").isin(tarja_vermelha))
    .select(F.col("EAN 1"), F.col("EAN 2"), F.col("EAN 3"))
    .rdd.flatMap(lambda row: [row["EAN 1"], row["EAN 2"], row["EAN 3"]])
    .filter(lambda x: x is not None)
    .distinct()
    .collect()
)

eans_tarja_preta = (
    medicamentos_anvisa_df.filter(F.col("TARJA").isin(tarja_preta))
    .select(F.col("EAN 1"), F.col("EAN 2"), F.col("EAN 3"))
    .rdd.flatMap(lambda row: [row["EAN 1"], row["EAN 2"], row["EAN 3"]])
    .filter(lambda x: x is not None)
    .distinct()
    .collect()
)

eans_tarja_sem_tarja = (
    medicamentos_anvisa_df.filter(F.col("TARJA").isin(sem_tarja))
    .select(F.col("EAN 1"), F.col("EAN 2"), F.col("EAN 3"))
    .rdd.flatMap(lambda row: [row["EAN 1"], row["EAN 2"], row["EAN 3"]])
    .filter(lambda x: x is not None)
    .distinct()
    .collect()
)

final_df = final_df.withColumn(
    "tarja",
    F.when(F.col("ean").isin(eans_tarja_vermelha), "Vermelha").otherwise(
        F.when(F.col("ean").isin(eans_tarja_preta), "Preta").otherwise(
            F.when(F.col("ean").isin(eans_tarja_sem_tarja), None).otherwise(
                F.col("tarja")
            )
        ),
    ),
)

print(
    f"Produtos sem tarja vazia: {final_df.filter(F.col('tarja').isNotNull()).count()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Salva no S3 e no delta table

# COMMAND ----------

final_df = final_df.withColumn(
    "gerado_em", F.coalesce("gerado_em", F.current_timestamp())
)

# COMMAND ----------

final_df.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(
    Table.produtos_em_processamento.value
)
