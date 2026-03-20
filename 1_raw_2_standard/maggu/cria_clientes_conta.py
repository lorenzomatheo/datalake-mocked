# Databricks notebook source
# MAGIC %md
# MAGIC # Cria `clientes_conta_standard`...
# MAGIC ...a partir do copilot

# COMMAND ----------

import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_clientes_conta_standard
from maggulake.utils.pyspark import to_schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Environment

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "cria_clientes_conta",
    dbutils,
    widgets={"full_refresh": ["false", "true"]},
)

spark = env.spark
full_refresh = dbutils.widgets.get("full_refresh") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê a tabela do postgres copilot

# COMMAND ----------

postgres = env.postgres_replica_adapter

clientes_df = postgres.read_table(spark, "clientes_cliente")
contas_df = postgres.read_table(spark, "contas_conta")

# clientes_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtra informações relevantes e adapta ao schema da tabela

# COMMAND ----------

filtered_df = clientes_df.select(
    [
        "id",
        "conta_id",
        "cod_externo",
        "cpf_cnpj",
        "nome_completo",
        "nome_social",
        "data_nascimento",
        "sexo",
        "email",
        "celular",
        "telefone_alt",
        "cep",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cidade",
        "estado",
        "autorizacao_comunicacao",
        "created_at",
        "updated_at",
    ]
)

# filtered_df.display()

# COMMAND ----------

# colocar campo "fonte" como sendo "copilot maggu"
final_df = filtered_df.withColumn("fonte", F.lit("copilot maggu"))

# criar um campo 'enderecos' a partir das colunas cep, logradouro, etc.
final_df = final_df.withColumn(
    "enderecos",
    F.array(
        F.struct(
            filtered_df.cep,
            filtered_df.logradouro,
            filtered_df.numero,
            filtered_df.complemento,
            filtered_df.bairro,
            filtered_df.cidade,
            filtered_df.estado,
        )
    ),
)

# Criar coluna separada para 'endereco_completo'
final_df = final_df.withColumn(
    "endereco_completo",
    F.concat_ws(
        " ",
        filtered_df.cep,
        filtered_df.logradouro,
        filtered_df.numero,
        filtered_df.complemento,
        filtered_df.bairro,
        filtered_df.cidade,
        filtered_df.estado,
    ),
)

# Drop colunas que não serão mais usadas (Enderecos)
final_df = final_df.drop(
    "cep", "logradouro", "numero", "complemento", "bairro", "cidade", "estado"
)

# criar coluna "conta" a partir da coluna "conta_id"
contas_df = contas_df.withColumnRenamed("id", "conta_conta_id")
final_df = final_df.join(
    contas_df.select("databricks_tenant_name", "conta_conta_id"),
    contas_df.conta_conta_id == final_df.conta_id,
    "left",
)
## renomear colunas
final_df = final_df.withColumnRenamed("databricks_tenant_name", "conta")
final_df = final_df.drop("conta_conta_id")

# renomear "created_at" para "criado_em" e "updated_at" para "atualizado_em"
final_df = final_df.withColumnRenamed("created_at", "criado_em")
final_df = final_df.withColumnRenamed("updated_at", "atualizado_em")

# atualizar o "atualizado_em" para agora
final_df = final_df.withColumn("atualizado_em", F.current_timestamp())

# cria campos vazios
final_df = final_df.withColumn("idade", F.lit(None).cast("int"))
final_df = final_df.withColumn("faixa_etaria", F.lit(None).cast("string"))

# COMMAND ----------

final_df = to_schema(final_df, schema_clientes_conta_standard)

# COMMAND ----------

# final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva a tabela no s3 e no delta

# COMMAND ----------

if full_refresh:
    # NOTE: o `full_refresh` é útil quando alteramos o schema da tabela,
    #       mas lembre-se de usar o liquibase pra registrar a migração
    final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        Table.clientes_conta_standard.value
    )
else:
    final_df.write.mode("overwrite").saveAsTable(Table.clientes_conta_standard.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testar se deu certo

# COMMAND ----------

teste_df = spark.read.table(Table.clientes_conta_standard.value)

teste_df.select("conta").distinct().display()
