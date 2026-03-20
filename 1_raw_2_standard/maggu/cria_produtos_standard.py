# Databricks notebook source
# MAGIC %pip install boto3 pandas fsspec s3fs xlrd pdfplumber
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime

import pandas as pd
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import types as T

from maggulake.enums import ClasseTerapeutica, Especialidades, TipoMedicamento
from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder, Table
from maggulake.mappings.anvisa_medicamentos_referencia import (
    AnvisaMedicamentosReferenciaParser,
)
from maggulake.produtos.from_produtos_loja import to_produtos
from maggulake.produtos.repository import ProdutosRepository
from maggulake.schemas import schema_produtos_standard
from maggulake.utils.pyspark import to_schema, verifica_coluna_completude
from maggulake.utils.strings import normalize_text_alphanumeric
from maggulake.utils.valores_invalidos import nullifica_valores_invalidos

env = DatabricksEnvironmentBuilder.build(
    "cria_produtos_standard",
    dbutils,
    widgets={
        "full_refresh": ["false", "true"],
        "clear_cache": ["false", "true", "false"],
    },
)

DEBUG = dbutils.widgets.get("debug") == "true"

full_refresh = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

clear_cache = dbutils.widgets.get("clear_cache")

if clear_cache == "true":
    spark.catalog.clearCache()
    print("Cache limpo")


# COMMAND ----------

# MAGIC %md ## Configurações

# COMMAND ----------

stage = env.settings.name_short
catalog = env.settings.catalog
s3_bucket_name = env.settings.bucket

spark = env.spark

s3_produtos_path = f"s3://{s3_bucket_name}/2-optimized-layer/produtos"
s3_produtos_loja_path = f"s3://{s3_bucket_name}/2-optimized-layer/produtos_loja"
s3_eh_medicamento_verificados = (
    f"s3://{s3_bucket_name}/3-refined-layer/produtos_eh_medicamento_verificados"
)

s3_folder_anvisa = f"s3://{s3_bucket_name}/1-raw-layer/maggu/consultas.anvisa.gov.br/info-medicamentos-e-otcs/"
s3_file_medicamentos = s3_folder_anvisa + "xls_conformidade_site_20240429_164029250.xls"

tabela_produtos_iqvia = f"{catalog}.raw.produtos_iqvia"
tabela_produtos_genericos = f"{catalog}.raw.produtos_genericos"

full_refresh = full_refresh or not spark.catalog.tableExists(
    Table.produtos_standard.value
)

# COMMAND ----------

# MAGIC %md ## Carrega produtos

# COMMAND ----------

# TODO: mover lista de EANs errados para config externa ou tabela de correções manuais.
eans_errados = [
    "7622300988470",  # WAFER RECHEIO E COBERTURA CHOCOLATE LACTA BIS XTRA PACOTE 45G
    "7895800151570",  # CHICLETE UVA BUBBALOO CAIXA 300G 60 UNIDADES
    "7895800151389",  # CHICLETE TUTTI FRUTTI BUBBALOO CAIXA 300G 60 UNIDADES
    "7895800300145",  # BALA CEREJA HALLS PACOTE 34G
    "7898591454462",  # CH.FINE C/XILITOL ZERO ACUCAR
    "7891000107836",  # CHOCOLATE AO LEITE AERADO SUFLAIR PACOTE 50G
]

# COMMAND ----------

produtos_iniciais = (
    env.table(Table.produtos_standard).cache()
    if not full_refresh
    else spark.createDataFrame([], schema_produtos_standard)
)

produtos_raw = (
    env.table(Table.produtos_raw).filter(~F.col("ean").isin(eans_errados)).drop("bula")
)

produtos_iniciais = (
    produtos_iniciais.unionByName(produtos_raw, allowMissingColumns=True)
    .dropDuplicates(["ean"])
    .withColumn("eans_alternativos", F.coalesce("eans_alternativos", F.array()))
    .withColumn("atualizado_em", F.current_timestamp())
    .cache()
)

# COMMAND ----------

produtos_loja_copilot = env.table(CopilotTable.produtos_loja).cache()
produtos_copilot = to_produtos(produtos_loja_copilot, produtos_iniciais.schema).cache()
ids_produtos_copilot = env.table(CopilotTable.produtos).select("id", "ean")

# COMMAND ----------

# MAGIC %md ### Join maroto
# MAGIC
# MAGIC Produtos são filtrados para incluir apenas aqueles que existem em pelo menos uma loja (produto_loja).
# MAGIC Produtos que vêm apenas das fontes raw (RD, Farmarcas, ConsultaRemedios, IQVIA, Anvisa, etc.)
# MAGIC mas não estão em nenhuma loja são excluídos para evitar processamento desnecessário.
# MAGIC
# MAGIC - Se o produto estiver no datalake e no copilot, pegamos o id do copilot e o resto do datalake
# MAGIC - Se o produto so estiver no copilot, pegamos ele inteiro de la
# MAGIC - Produtos apenas no datalake (sem loja) são filtrados e não entram no pipeline

# COMMAND ----------

produtos_copilot_e_datalake = (
    produtos_iniciais.drop("id")
    .alias("pi")
    .join(F.broadcast(produtos_copilot.select("id", "ean").alias("pc")), "ean", "inner")
    .select("pc.id", "pi.*")
).dropDuplicates(["ean"])

produtos_so_copilot = produtos_copilot.join(
    F.broadcast(produtos_iniciais.select("ean")),
    "ean",
    "left_anti",
)

produtos_so_datalake = produtos_iniciais.join(
    F.broadcast(produtos_copilot.select("ean")),
    "ean",
    "left_anti",
)

produtos_so_datalake = (
    produtos_so_datalake.drop("id")
    .alias("p")
    .join(
        F.broadcast(ids_produtos_copilot).alias("i"),
        "ean",
        "left",
    )
    .selectExpr(
        "p.*",
        "COALESCE(i.id, uuid()) AS id",
    )
)

# COMMAND ----------

produtos_todos = produtos_copilot_e_datalake.unionByName(
    produtos_so_copilot, allowMissingColumns=True
)

if DEBUG:
    print(f"Produtos no datalake e copilot: {produtos_copilot_e_datalake.count()}")
    print(f"Produtos so no copilot: {produtos_so_copilot.count()}")
    print(f"Produtos filtrados (só datalake, sem loja): {produtos_so_datalake.count()}")
    print(f"Total de produtos para processamento: {produtos_todos.count()}")

if full_refresh:
    pra_salvar = produtos_todos
else:
    pra_salvar = produtos_todos.join(
        F.broadcast(env.table(Table.produtos_standard).select("ean")),
        "ean",
        "left_anti",
    )

pra_salvar.cache()

# COMMAND ----------

# MAGIC %md ## Arruma eans errados

# COMMAND ----------

correcoes = [
    {
        "ean": "7622300988470",
        "nome": "Inalador Pulmopar Pneumático A Jato De Ar - Soniclear",
        "ean_correto": "7896615113333",
    }
]


def condicao(c):
    return (F.col("ean") == c["ean"]) & (F.col("nome") == c["nome"])


coluna_nova_ean = F.when(condicao(correcoes[0]), correcoes[0]["ean_correto"])

for c in correcoes[1:]:
    coluna_nova_ean = coluna_nova_ean.when(condicao(c), c["ean_correto"])

coluna_nova_ean = coluna_nova_ean.otherwise(F.col("ean"))

pra_salvar = pra_salvar.withColumn("ean", coluna_nova_ean)

pra_salvar.filter("nome = 'Reconter 20mg, caixa com 30 comprimidos revestidos'").select(
    "ean", "nome"
).show(truncate=False)

# COMMAND ----------

correcao = pra_salvar.filter("ean = '7899095236400'").withColumn(
    "ean", F.lit("7899095260481")
)

pra_salvar = pra_salvar.filter("ean != '7899095260481'").unionByName(correcao)

# COMMAND ----------

# Ignora 0s a esquerda nos EANs pois sao irrelevantes
pra_salvar = pra_salvar.withColumn("ean", F.regexp_replace(F.col("ean"), "^[0]+", ""))
# display(pra_salvar)

# COMMAND ----------

# Para eans duplicados, seleciona o registro com a maior completude
pra_salvar = verifica_coluna_completude(
    df=pra_salvar,
    partition_by_cols=["ean"],
    tie_break_cols=[F.col("id")],
)

# COMMAND ----------

# MAGIC %md ## Atualiza nomes alternativos

# COMMAND ----------

produtos_loja = env.table(CopilotTable.produtos_loja).cache()

nomes_alternativos = (
    produtos_loja.select(
        "ean", F.get_json_object("substituicoes_produto_v2", "$.nome").alias("nome")
    )
    .distinct()
    .groupBy("ean")
    .agg(F.collect_list("nome").alias("nomes_alternativos"))
)

pra_salvar = (
    pra_salvar.drop("nomes_alternativos")
    .alias("p")
    .join(nomes_alternativos, "ean", "left")
    .selectExpr("p.*", "nomes_alternativos")
)

# COMMAND ----------

pra_salvar = pra_salvar.withColumn(
    "atualizado_em", F.coalesce("atualizado_em", F.current_timestamp())
).withColumn("gerado_em", F.coalesce("gerado_em", F.current_timestamp()))

# COMMAND ----------

pr = ProdutosRepository(spark, stage, catalog)

# COMMAND ----------

com_ids = pr.com_ids(pra_salvar)

pra_salvar_temp = to_schema(com_ids, schema_produtos_standard)
temp_path = (
    f"dbfs:/tmp/maggu/products_with_schema_{datetime.now().strftime('%Y%m%d%H%M%S')}"
)
pra_salvar_temp.write.mode("overwrite").parquet(temp_path)
pra_salvar = spark.read.parquet(temp_path).cache()

display(pra_salvar)

# COMMAND ----------

pra_salvar.schema

# COMMAND ----------

# MAGIC %md ## Corrige campos `eh_medicamento`
# MAGIC

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

# Lê tabela de produtos verificados manualmente
df = spark.read.format("delta").load(s3_eh_medicamento_verificados)

tarjas = ["Tarja Vermelha", "Tarja Vermelha sob restrição", "Tarja Preta"]
sem_tarja = ["Tarja Sem Tarja"]
sem_info_tarja = ["- (*) "]

# Lista de EANS que devem ser marcados como `eh_medicamento = True` e `eh_tarjado = True`
eans_from_medicamentos_df = (
    medicamentos_anvisa_df.filter(F.col("TARJA").isin(tarjas))
    .select(F.col("EAN 1"), F.col("EAN 2"), F.col("EAN 3"))
    .rdd.flatMap(lambda row: [row["EAN 1"], row["EAN 2"], row["EAN 3"]])
    .filter(lambda x: x is not None)
)

eans_from_df = (
    df.filter((F.col("eh_medicamento") == True) & (F.col("eh_tarjado") == True))
    .select("ean")
    .rdd.flatMap(lambda row: [row["ean"]])
)

eans_medicamentos_tarjados = (
    eans_from_df.union(eans_from_medicamentos_df).distinct().collect()
)

print(
    len(eans_medicamentos_tarjados),
    " produtos para serem marcados como medicamento e tarjado",
)

# Lista de EANS que devem ser marcados como `eh_medicamento = True` e `eh_tarjado = False`
eans_nao_tarjados_from_medicamentos_df = (
    medicamentos_anvisa_df.filter(F.col("TARJA").isin(sem_tarja))
    .select(F.col("EAN 1"), F.col("EAN 2"), F.col("EAN 3"))
    .rdd.flatMap(lambda row: [row["EAN 1"], row["EAN 2"], row["EAN 3"]])
    .filter(lambda x: x is not None)
)

eans_nao_tarjados_from_df = (
    df.filter((F.col("eh_medicamento") == True) & (F.col("eh_tarjado") == False))
    .select("ean")
    .rdd.flatMap(lambda row: [row["ean"]])
)

eans_medicamentos_nao_tarjados = (
    eans_nao_tarjados_from_df.union(eans_nao_tarjados_from_medicamentos_df)
    .distinct()
    .collect()
)

# COMMAND ----------

print(
    len(eans_medicamentos_nao_tarjados),
    " produtos para serem marcados como medicamento não tarjado",
)

# COMMAND ----------

# Lista de EANS que devem ser marcados como `eh_medicamento = True` e `eh_tarjado is null`
eans_medicamentos_tarjados_null = (
    df.filter((F.col("eh_medicamento") == True) & (F.col("eh_tarjado").isNull()))
    .select("ean")
    .rdd.flatMap(lambda x: x)
    .distinct()
    .collect()
)

# COMMAND ----------

print(
    len(eans_medicamentos_tarjados_null),
    " produtos para serem marcados como medicamento e tarjado nulo",
)

# COMMAND ----------

# Lista de EANS que devem ser marcados como `eh_medicamento = True` mas não temos info sobre tarja
eans_medicamentos_sem_info_tarja = (
    medicamentos_anvisa_df.filter(F.col("TARJA").isin(sem_info_tarja))
    .select(F.col("EAN 1"), F.col("EAN 2"), F.col("EAN 3"))
    .rdd.flatMap(lambda row: [row["EAN 1"], row["EAN 2"], row["EAN 3"]])
    .filter(lambda x: x is not None)
    .distinct()
    .collect()
)

print(
    len(eans_medicamentos_sem_info_tarja),
    " produtos para serem marcados como medicamento",
)

print(
    "Total: ",
    len(eans_medicamentos_tarjados)
    + len(eans_medicamentos_nao_tarjados)
    + len(eans_medicamentos_tarjados_null)
    + len(eans_medicamentos_sem_info_tarja),
)


# COMMAND ----------

eans_genericos = (
    spark.read.table(tabela_produtos_genericos)
    .select("ean")
    .rdd.flatMap(lambda row: [str(row["ean"])])
    .collect()
)

# COMMAND ----------

eans_medicamentos = (
    eans_medicamentos_tarjados
    + eans_medicamentos_nao_tarjados
    + eans_medicamentos_tarjados_null
    + eans_medicamentos_sem_info_tarja
    + eans_genericos
)

# remove duplicados
eans_medicamentos = list(set(eans_medicamentos))

# aplica correções para o campo "eh_medicamento"
pra_salvar = pra_salvar.withColumn(
    "eh_medicamento",
    F.when(F.col("ean").isin(eans_medicamentos), True).otherwise(
        F.col("eh_medicamento")
    ),
)

# COMMAND ----------

# aplica correções para o campo "eh_tarjado"
pra_salvar = pra_salvar.withColumn(
    "eh_tarjado",
    F.when(F.col("ean").isin(eans_medicamentos_tarjados), True).otherwise(
        F.when(F.col("ean").isin(eans_medicamentos_nao_tarjados), False).otherwise(
            F.when(F.col("ean").isin(eans_medicamentos_tarjados_null), None).otherwise(
                F.col("eh_tarjado")
            )
        ),
    ),
)

# COMMAND ----------

# aplica correções para o campo "tipo_medicamento"
pra_salvar = pra_salvar.withColumn(
    "tipo_medicamento",
    F.when(F.col("ean").isin(eans_genericos), TipoMedicamento.GENERICO.value).otherwise(
        F.col("tipo_medicamento")
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualizando com info da IQVIA

# COMMAND ----------

produtos_iqvia = spark.read.table(tabela_produtos_iqvia)

contagem_antes = pra_salvar.select(
    F.count(
        F.when(F.col("eh_medicamento").isNull() | (F.col("eh_medicamento") == ""), True)
    ).alias("contagem_nulos_eh_medicamento"),
    F.count(
        F.when(
            F.col("principio_ativo").isNull() | (F.col("principio_ativo") == ""), True
        )
    ).alias("contagem_nulos_principio_ativo"),
    F.count(F.when(F.col("marca").isNull() | (F.col("marca") == ""), True)).alias(
        "contagem_nulos_marca"
    ),
    F.count(F.when(F.col("categorias").isNull(), True)).alias(
        "contagem_nulos_categorias"
    ),
).collect()[0]

joined_df = pra_salvar.join(
    produtos_iqvia, pra_salvar["ean"] == produtos_iqvia["cd_ean"], "left"
)

# COMMAND ----------


# TODO: DRY — update_categorias, update_especialidades e update_classes_terapeuticas fazem a mesma coisa.
# TODO: criar factory function em maggulake/utils/. Adicionar type hints.
def update_categorias(categorias, cc_1_desc):
    if cc_1_desc is None or cc_1_desc == "NOT OTC":
        return categorias
    if categorias is None:
        return [cc_1_desc]
    if cc_1_desc not in categorias:
        categorias.append(cc_1_desc)
    return categorias


update_categorias_udf = F.udf(update_categorias, T.ArrayType(T.StringType()))

updated_df = (
    joined_df.withColumn(
        "eh_medicamento",
        F.when(
            F.col("eh_medicamento").isNull() | (F.col("eh_medicamento") == ""),
            F.col("fg_medicamento"),
        ).otherwise(F.col("eh_medicamento")),
    )
    .withColumn(
        "principio_ativo",
        F.when(
            F.col("principio_ativo").isNull() | (F.col("principio_ativo") == ""),
            F.col("molecula_br"),
        ).otherwise(F.col("principio_ativo")),
    )
    .withColumn(
        "marca",
        F.when(
            F.col("marca").isNull() | (F.col("marca") == ""),
            F.col("produto"),
        ).otherwise(F.col("marca")),
    )
    .withColumn(
        "categorias", update_categorias_udf(F.col("categorias"), F.col("cc_1_desc"))
    )
    .withColumn(
        "tipo_medicamento",
        F.when(
            F.col("tipo_medicamento").isNull() | (F.col("tipo_medicamento") == ""),
            F.when(F.col("gmrs") == "G", TipoMedicamento.GENERICO.value)
            .when(F.col("gmrs") == "R", TipoMedicamento.REFERENCIA.value)
            .when(F.col("gmrs") == "S", TipoMedicamento.SIMILAR.value),
        ).otherwise(F.col("tipo_medicamento")),
    )
)

produtos_iqvia_columns = produtos_iqvia.columns
updated_df = updated_df.drop(*produtos_iqvia_columns)

contagem_depois = updated_df.select(
    F.count(
        F.when(F.col("eh_medicamento").isNull() | (F.col("eh_medicamento") == ""), True)
    ).alias("contagem_nulos_eh_medicamento"),
    F.count(
        F.when(
            F.col("principio_ativo").isNull() | (F.col("principio_ativo") == ""), True
        )
    ).alias("contagem_nulos_principio_ativo"),
    F.count(F.when(F.col("marca").isNull() | (F.col("marca") == ""), True)).alias(
        "contagem_nulos_marca"
    ),
    F.count(F.when(F.col("categorias").isNull(), True)).alias(
        "contagem_nulos_categorias"
    ),
).collect()[0]

# COMMAND ----------

print("Contagem de valores nulos ou vazios antes da atualização:")
print(f"eh_medicamento: {contagem_antes['contagem_nulos_eh_medicamento']}")
print(f"principio_ativo: {contagem_antes['contagem_nulos_principio_ativo']}")
print(f"marca: {contagem_antes['contagem_nulos_marca']}")
print(f"categorias: {contagem_antes['contagem_nulos_categorias']}")

print("\nContagem de valores nulos ou vazios depois da atualização:")
print(f"eh_medicamento: {contagem_depois['contagem_nulos_eh_medicamento']}")
print(f"principio_ativo: {contagem_depois['contagem_nulos_principio_ativo']}")
print(f"marca: {contagem_depois['contagem_nulos_marca']}")
print(f"categorias: {contagem_depois['contagem_nulos_categorias']}")

# COMMAND ----------

pra_salvar = updated_df

# COMMAND ----------

# inferência de tipo_medicamento GENÉRICO a partir do nome do produto
# Genéricos no Brasil são obrigados a usar o princípio ativo (DCI) como nome comercial
normalize_udf = F.udf(normalize_text_alphanumeric, T.StringType())

pra_salvar = pra_salvar.withColumn(
    "tipo_medicamento",
    F.when(
        (F.col("tipo_medicamento").isNull() | (F.col("tipo_medicamento") == ""))
        & (F.col("eh_medicamento") == True)
        & F.col("principio_ativo").isNotNull()
        & (F.length(F.trim(F.col("principio_ativo"))) > 4)
        & normalize_udf(F.col("nome")).startswith(
            normalize_udf(F.trim(F.col("principio_ativo")))
        ),
        F.lit(TipoMedicamento.GENERICO.value),
    ).otherwise(F.col("tipo_medicamento")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Padroniza campos com Enum

# COMMAND ----------

especialidades_set = set(Especialidades.list())
classes_terapeuticas_set = set(ClasseTerapeutica.list())


@F.udf(T.ArrayType(T.StringType()))
def update_especialidades(especialidades):
    if especialidades and isinstance(especialidades, list):
        items_validos = [item for item in especialidades if item in especialidades_set]
        return [items_validos[0]] if items_validos else None
    return None


@F.udf(T.ArrayType(T.StringType()))
def update_classes_terapeuticas(classes_terapeuticas):
    if classes_terapeuticas and isinstance(classes_terapeuticas, list):
        items_validos = [
            item for item in classes_terapeuticas if item in classes_terapeuticas_set
        ]
        return [items_validos[0]] if items_validos else None
    return None


pra_salvar = pra_salvar.withColumn(
    "especialidades", update_especialidades(F.col("especialidades"))
)
pra_salvar = pra_salvar.withColumn(
    "classes_terapeuticas", update_classes_terapeuticas(F.col("classes_terapeuticas"))
)
pra_salvar.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preenche medicamentos referência com Anvisa

# COMMAND ----------

anvisa_medicamentos_referencia_parser = (
    AnvisaMedicamentosReferenciaParser.from_default_urls(spark)
)

# COMMAND ----------

colunas_pra_atualizar = [
    "tipo_medicamento",
    "fabricante",
    "dosagem",
    "principio_ativo",
]

colunas_manter = [
    f"p.{col}" for col in pra_salvar.columns if col not in colunas_pra_atualizar
]

pra_salvar = (
    pra_salvar.alias("p")
    .join(
        anvisa_medicamentos_referencia_parser.df.alias("a_reg"),
        F.col("p.numero_registro") == F.col("a_reg.numero_registro"),
        "left",
    )
    .join(
        anvisa_medicamentos_referencia_parser.df.alias("a_marca"),
        (F.col("a_reg.numero_registro").isNull())
        & (F.col("p.marca") == F.col("a_marca.marca")),
        "left",
    )
    .select(
        *colunas_manter,
        *[
            F.coalesce(f"a_reg.{col}", f"a_marca.{col}", f"p.{col}").alias(col)
            for col in colunas_pra_atualizar
        ],
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preenche com medicamentos_anvisa (JSON API) por numero_registro

# COMMAND ----------

_medicamentos_anvisa = env.table(Table.medicamentos_anvisa).cache()

_colunas_comuns = [
    c
    for c in _medicamentos_anvisa.columns
    if c != "numero_registro" and c in pra_salvar.columns
]

pra_salvar = (
    pra_salvar.alias("p")
    .join(_medicamentos_anvisa.alias("ma"), "numero_registro", "left")
    .select(
        *[f"p.{c}" for c in pra_salvar.columns if c not in _colunas_comuns],
        *[F.coalesce(f"p.{c}", f"ma.{c}").alias(c) for c in _colunas_comuns],
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preenche campo bula usando tabela centralizada `bulas_medicamentos`

# COMMAND ----------

bulas = spark.table(Table.bulas_medicamentos.value)
bulas_text = (
    bulas.withColumn(
        "bula_texto", F.coalesce("bula_profissional", "bula_paciente")
    )  # preferência por bula profissional
    .filter(F.col("bula_texto").isNotNull())
    .groupBy("ean")
    .agg(
        F.max_by("bula_texto", F.length("bula_texto")).alias("bula_texto")
    )  # preferência por bula mais longa
)

pra_salvar = pra_salvar.drop("bula").join(bulas_text, on="ean", how="left")
pra_salvar = pra_salvar.withColumnRenamed("bula_texto", "bula")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva a tabela

# COMMAND ----------

pra_salvar = nullifica_valores_invalidos(pra_salvar)

# COMMAND ----------

pra_salvar = verifica_coluna_completude(
    df=pra_salvar,
    partition_by_cols=["ean"],
    tie_break_cols=[F.col("id")],
)

# COMMAND ----------

# se full_refresh atualiza tudo, se for incremental faz upsert
if full_refresh:
    pra_salvar.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        Table.produtos_standard.value,
    )
else:
    delta_table = DeltaTable.forName(spark, Table.produtos_standard.value)

    delta_table.alias("target").merge(
        pra_salvar.alias("source"), "target.ean = source.ean"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
