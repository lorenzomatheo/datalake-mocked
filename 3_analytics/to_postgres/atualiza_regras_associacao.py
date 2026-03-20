# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza a tabela das regras de associação de produtos
# MAGIC
# MAGIC Este notebook lê as regras de associação de produtos do datalake e atualiza
# MAGIC a tabela correspondente no Postgres.

# COMMAND ----------

# ambiente
dbutils.widgets.dropdown("stage", "prod", ["dev", "prod"])

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])
spark = (
    SparkSession.builder.appName("atualiza_regras_associacao")
    .config(conf=config)
    .getOrCreate()
)

# delta tables
PRODUCT_ASSOCIATION_RULES = f"{catalog}.analytics.product_association_rules"

# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, POSTGRES_USER, POSTGRES_PASSWORD)
tabela_regras_associacao = 'recomendacoes_regrasassociacaoprodutos'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do datalake

# COMMAND ----------

regras_associacao = spark.read.table(PRODUCT_ASSOCIATION_RULES).cache()

# COMMAND ----------

regras_associacao.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratamento dos dados

# COMMAND ----------

regras_associacao_formatadas = regras_associacao.selectExpr(
    'uuid() as id',
    'produto_a_id',
    'produto_b_id',
    'produto_a_ean',
    'produto_b_ean',
    'produto_a_nome',
    'produto_b_nome',
    'support',
    'confidence',
    'lift',
    'conviction',
    'cast(total_transacoes as int) as total_transacoes',
    'cast(transacoes_produto_a as int) as transacoes_produto_a',
    'cast(transacoes_produto_b as int) as transacoes_produto_b',
    'cast(transacoes_ambos_produtos as int) as transacoes_ambos_produtos',
    'algoritmo_usado',
    'periodo_analise_dias',
    'data_calculo',
    'current_timestamp() as criado_em',
    'current_timestamp() as atualizado_em',
)

# COMMAND ----------

# Validação de dados obrigatórios
regras_com_dados_faltantes = regras_associacao_formatadas.filter(
    F.col("produto_a_id").isNull()
    | F.col("produto_b_id").isNull()
    | F.col("produto_a_ean").isNull()
    | F.col("produto_b_ean").isNull()
    | F.col("support").isNull()
    | F.col("confidence").isNull()
    | F.col("lift").isNull()
)

if (c := regras_com_dados_faltantes.count()) > 0:
    print(
        f"ATENÇÃO: Encontradas {c} linhas com dados obrigatórios faltantes. "
        f"Estas linhas serão removidas:"
    )
    display(regras_com_dados_faltantes)

    # Remover linhas com dados obrigatórios faltantes
    regras_associacao_formatadas = regras_associacao_formatadas.filter(
        F.col("produto_a_id").isNotNull()
        & F.col("produto_b_id").isNotNull()
        & F.col("produto_a_ean").isNotNull()
        & F.col("produto_b_ean").isNotNull()
        & F.col("support").isNotNull()
        & F.col("confidence").isNotNull()
        & F.col("lift").isNotNull()
    )
else:
    print("Ótimo! Todos os dados obrigatórios estão presentes.")

# COMMAND ----------

chave_unica = ["produto_a_id", "produto_b_id", "data_calculo", "algoritmo_usado"]

regras_associacao_deduplicadas = regras_associacao_formatadas.dropDuplicates(
    chave_unica
)

print(
    f"Inserindo {regras_associacao_deduplicadas.count()} regras de associação no Postgres..."
)

# COMMAND ----------

postgres.insert_into_table(
    regras_associacao_deduplicadas, tabela_regras_associacao, mode='overwrite'
)

print("Processo concluído com sucesso!")
