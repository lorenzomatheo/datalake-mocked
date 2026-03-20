# Databricks notebook source
# TODO: migrar para DatabricksEnvironmentBuilder em vez de widgets/SparkConf manuais.
dbutils.widgets.dropdown("catalog", "staging", ["staging", "production"])
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

catalog = dbutils.widgets.get('catalog')
full_refresh = dbutils.widgets.get('full_refresh') == 'true'

# COMMAND ----------

FONTE = 'ClientesEnriquecidos'

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("acha_remedios_de_produtos_nao_enriquecidos")
    .config(conf=config)
    .getOrCreate()
)


# COMMAND ----------

if full_refresh:
    spark.sql(f"DELETE FROM {catalog}.raw.produtos_raw WHERE fonte = '{FONTE}'")

# COMMAND ----------

produtos = spark.read.table(f'{catalog}.standard.produtos_standard').cache()
produtos_raw = spark.read.table(f'{catalog}.raw.produtos_raw').cache()

# COMMAND ----------

produtos_novos = (
    produtos.join(produtos_raw, 'ean', 'leftanti')
    .select(
        'ean',
        'nome',
        'imagem_url',
        'arquivo_s3',
    )
    .cache()
)

produtos_novos

# COMMAND ----------

# Remedios que dao matches aleatorios, como por exemplo muitas escovas de dente vem com nome abreviado "Esc"
remedios_malvados = ['Esc', 'Rosa']

remedios = (
    produtos_raw.dropna(subset='marca')
    .dropDuplicates(['marca'])
    .select(
        'marca',
        'fabricante',
        'descricao',
        'eh_medicamento',
        'tipo_medicamento',
        'principio_ativo',
        'eh_tarjado',
        'categorias',
        'classes_terapeuticas',
        'especialidades',
        'tipo_de_receita_completo',
        'bula',
    )
    .filter(~F.col('marca').isin(remedios_malvados))
    .cache()
)

remedios

# COMMAND ----------

remedios.filter("marca ilike 'esc'").count()

# COMMAND ----------

print(produtos_novos.count())
print(remedios.count())

# COMMAND ----------

produtos_raw_novos = (
    remedios.alias('r')
    .join(
        F.broadcast(produtos_novos.alias('p')),
        F.expr("lower(p.nome) rlike concat('^', lower(r.marca), '[ $]')"),
    )
    .cache()
)

display(produtos_raw_novos)

# COMMAND ----------

produtos_raw_novos.count()

# COMMAND ----------

display(produtos_raw_novos.groupBy('marca').count().orderBy('count', ascending=False))

# COMMAND ----------

display(produtos_raw_novos.filter("nome ilike '%fluimucil%'"))

# COMMAND ----------

produtos_raw_novos.withColumn('fonte', F.lit(FONTE)).write.mode('append').saveAsTable(
    f'{catalog}.raw.produtos_raw'
)
