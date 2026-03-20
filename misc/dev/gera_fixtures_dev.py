# Databricks notebook source
import json

import boto3
import pyspark.sql.functions as F

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# postgres
USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')

# seed
ID_CONTA_MAGGU = '8369aa2a-43f9-4ddf-9ff4-3928d938f64c'
ID_LOJA_MAGGU = 'eae52186-3700-4de4-9a14-d9c6829ec138'


# COMMAND ----------

postgres_adapter = PostgresAdapter('prod', user=USER, password=PASSWORD)

# COMMAND ----------

eans = spark.read.table('production.utils.eans_dev')
eans_list = [e.ean for e in eans.collect()]

eans.display()

# COMMAND ----------


def get_schema(table_name):
    return postgres_adapter.execute_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name = '{table_name}';"
    )


def to_fixture(row, model_name, pk_col, schema):
    row_dict = row.asDict()

    for _, column in schema.iterrows():
        if column.data_type == "jsonb":
            row_dict[column.column_name] = (
                json.loads(row_dict[column.column_name])
                if row_dict[column.column_name] is not None
                else None
            )
        elif column.data_type == "numeric":
            row_dict[column.column_name] = (
                str(row_dict[column.column_name])
                if row_dict[column.column_name] is not None
                else None
            )
        elif column.data_type == "timestamp with time zone":
            row_dict[column.column_name] = (
                row_dict[column.column_name].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                if row_dict[column.column_name]
                else None
            )

    return {
        "model": model_name,
        "pk": row_dict[pk_col],
        "fields": {k: v for k, v in row_dict.items() if k != pk_col},
    }


def to_fixture_fn(model_name, pk_col, schema):
    return lambda row: to_fixture(row, model_name, pk_col, schema)


# COMMAND ----------

# MAGIC %md Atencao, table_name e com "_" e model_name e com "."

# COMMAND ----------

produtos = postgres_adapter.read_table(spark, 'produtos_produtov2').filter(
    F.col("ean").isin(eans_list)
)

produtos_fixtures = produtos.rdd.map(
    to_fixture_fn('produtos.produtov2', 'id', get_schema('produtos_produtov2'))
).collect()

produtos_fixtures[1]

# COMMAND ----------

ids_produtos = [f['pk'] for f in produtos_fixtures]
produtos_loja = (
    postgres_adapter.read_table(spark, 'produtos_produtoloja')
    .filter("tenant = 'toolspharma' and codigo_loja = 'prototipo'")
    .filter(F.col("produto_id").isin(ids_produtos))
    .withColumn('tenant', F.lit('maggu'))
    .withColumn('codigo_loja', F.lit('maggu'))
    .withColumn('loja_id', F.lit(ID_LOJA_MAGGU))
    .withColumn(
        'ean_conta_loja',
        F.concat('ean', F.lit('|'), 'tenant', F.lit('|'), 'codigo_loja'),
    )
)

produtos_loja_fixtures = produtos_loja.rdd.map(
    to_fixture_fn('produtos.produtoloja', 'id', get_schema('produtos_produtoloja'))
).collect()

produtos_loja_fixtures[1]

# COMMAND ----------

campanhas = (
    postgres_adapter.read_table(spark, 'campanhas_campanha')
    .withColumn("criado_por_id", F.lit(None).cast("string"))
    .withColumn("atualizado_por_id", F.lit(None).cast("string"))
)

campanhas_fixtures = campanhas.rdd.map(
    to_fixture_fn('campanhas.campanha', 'id', get_schema('campanhas_campanha'))
).collect()

campanhas_fixtures[0]

# COMMAND ----------

campanhas_produtos = postgres_adapter.read_table(
    spark, 'campanhas_campanhaproduto'
).filter(F.col("produto_id").isin(ids_produtos))

campanhas_produtos_fixtures = campanhas_produtos.rdd.map(
    to_fixture_fn(
        'campanhas.campanhaproduto', 'id', get_schema('campanhas_campanhaproduto')
    )
).collect()

campanhas_produtos_fixtures[0]

# COMMAND ----------

campanhas_contas = (
    postgres_adapter.read_table(spark, 'campanhas_campanha_todas_as_lojas_das_contas')
    .dropDuplicates(['campanha_id'])
    .withColumn('conta_id', F.lit(ID_CONTA_MAGGU))
)

campanhas_contas_fixtures = campanhas_contas.rdd.map(
    to_fixture_fn(
        'campanhas.campanha_todas_as_lojas_das_contas',
        'id',
        get_schema('campanhas_campanha_todas_as_lojas_das_contas'),
    )
).collect()

campanhas_contas_fixtures[0]

# COMMAND ----------

campanhas_lojas = (
    postgres_adapter.read_table(spark, 'campanhas_campanha_lojas_especificas')
    .dropDuplicates(['campanha_id'])
    .withColumn('loja_id', F.lit(ID_LOJA_MAGGU))
)
campanhas_lojas_fixtures = campanhas_lojas.rdd.map(
    to_fixture_fn(
        'campanhas.campanha_lojas_especificas',
        'id',
        get_schema('campanhas_campanha_lojas_especificas'),
    )
).collect()

campanhas_lojas_fixtures[0]

# COMMAND ----------

fixtures = (
    produtos_fixtures
    + produtos_loja_fixtures
    + campanhas_fixtures
    + campanhas_produtos_fixtures
    + campanhas_contas_fixtures
    + campanhas_lojas_fixtures
)

if len(fixtures) == 0:
    dbutils.notebook.exit("fixtures estao vazios e não serao salvos.")

# COMMAND ----------

s3_client = boto3.client("s3")

raw = json.dumps(fixtures, default=str).encode()
s3_client.put_object(
    Body=raw,
    Bucket='maggu-datalake-dev',
    Key='5-sharing-layer/copilot.localhost/fixtures_dev.json',
)
