# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import math

import pandas as pd
import pyspark.sql.functions as F
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter
from maggulake.utils.df_utils import escape_string

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("salva_clientes_refinados_copilot")
    .config(conf=config)
    .getOrCreate()
)

STAGE = dbutils.widgets.get("stage")
CATALOG = "production" if STAGE == "prod" else "staging"
USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")

tabela_clientes_conta = f"{CATALOG}.refined.clientes_conta_refined"

postgres = PostgresAdapter(STAGE, USER, PASSWORD)
postgres_connection = postgres.get_connection()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê a tabela de clientes do Postgres

# COMMAND ----------

postgres.read_table(spark, "clientes_cliente").limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega e prepara os dados dos clientes refinados

# COMMAND ----------

clientes_refinados = spark.read.table(tabela_clientes_conta)  # .cache()
clientes_refinados.limit(100).display()

# COMMAND ----------

# seleciona a linha mais preenchida para cada cpf
clientes_unicos = (
    clientes_refinados.withColumn(
        "non_null_count",
        sum(F.col(c).isNotNull().cast("int") for c in clientes_refinados.columns),
    )
    .orderBy(F.desc("non_null_count"))
    .dropDuplicates(subset=["cpf_cnpj"])
)

# poderia pegar a mais recente, mas nos meus testes veio menos preenchida que a opção acima
# clientes_unicos = clientes_refinados.orderBy(F.desc("atualizado_em")).dropDuplicates(subset=["cpf_cnpj"])

clientes_unicos.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa query de update

# COMMAND ----------


def create_value_string(row):
    # pega as informações de uma linha do dataframe para passar na query
    return f"""(
        {f"'{escape_string(row.cod_externo)}'" if pd.notna(row.cod_externo) else 'NULL'},
        {f"'{escape_string(row.cpf_cnpj)}'" if pd.notna(row.cpf_cnpj) else 'NULL'},
        {str(row.eh_pessoa_fisica).lower() if pd.notna(row.eh_pessoa_fisica) else 'false'},
        {str(row.eh_pessoa_juridica).lower() if pd.notna(row.eh_pessoa_juridica) else 'false'},
        {f"'{escape_string(row.nome_completo)}'" if pd.notna(row.nome_completo) else 'NULL'},
        {f"'{row.data_nascimento}'::date" if pd.notna(row.data_nascimento) else 'NULL'},
        {f"'{escape_string(row.sexo)}'" if pd.notna(row.sexo) else 'NULL'},
        {f"'{escape_string(row.email)}'" if pd.notna(row.email) else 'NULL'},
        {f"'{escape_string(row.celular)}'" if pd.notna(row.celular) else 'NULL'},
        {f"'{escape_string(row.telefone_alt)}'" if pd.notna(row.telefone_alt) else 'NULL'},
        {f"'{escape_string(row.endereco.get('cep'))}'" if row.endereco and row.endereco.get('cep') else 'NULL'},
        {f"'{escape_string(row.endereco.get('logradouro'))}'" if row.endereco and row.endereco.get('logradouro') else 'NULL'},
        {f"'{escape_string(row.endereco.get('numero'))}'" if row.endereco and row.endereco.get('numero') else 'NULL'},
        {f"'{escape_string(row.endereco.get('complemento'))}'" if row.endereco and row.endereco.get('complemento') else 'NULL'},
        {f"'{escape_string(row.endereco.get('bairro'))}'" if row.endereco and row.endereco.get('bairro') else 'NULL'},
        {f"'{escape_string(row.endereco.get('cidade'))}'" if row.endereco and row.endereco.get('cidade') else 'NULL'},
        {f"'{escape_string(row.endereco.get('estado'))}'" if row.endereco and row.endereco.get('estado') else 'NULL'},
        {f"'{escape_string(row.enderecos_json)}'" if pd.notna(row.enderecos_json) else 'NULL'},
        {str(row.autorizacao_comunicacao).lower() if pd.notna(row.autorizacao_comunicacao) else 'false'},
        {f"'{{{escape_string(row.doencas_cronicas)}}}'" if pd.notna(row.doencas_cronicas) else 'NULL'},
        {f"'{{{escape_string(row.doencas_agudas)}}}'" if pd.notna(row.doencas_agudas) else 'NULL'},
        {f"'{{{escape_string(row.medicamentos_controle)}}}'" if pd.notna(row.medicamentos_controle) else 'NULL'},
        {f"'{escape_string(row.fonte)}'" if pd.notna(row.fonte) else 'NULL'},
        CURRENT_TIMESTAMP
    )"""


# COMMAND ----------


def update_clientes_in_batches(df, postgres, batch_size: int = 1000):
    # Converte array e JSON
    df_prepared = (
        df.withColumn("doencas_cronicas", F.array_join("doencas_cronicas", ","))
        .withColumn("doencas_agudas", F.array_join("doencas_agudas", ","))
        .withColumn("medicamentos_controle", F.array_join("medicamentos_controle", ","))
        .withColumn("endereco", F.col("enderecos")[0])
        .withColumn("enderecos_json", F.to_json("enderecos"))
    )

    df_pandas = df_prepared.toPandas()  # TODO não converter para pandas
    total_rows = len(df_pandas)
    num_batches = math.ceil(total_rows / batch_size)

    update_base_query = """
    WITH updated_values (
        cod_externo, cpf_cnpj, eh_pessoa_fisica, eh_pessoa_juridica,
        nome_completo, data_nascimento, sexo, email, celular,
        telefone_alt, cep, logradouro, numero, complemento, bairro, cidade,
        estado, enderecos, autorizacao_comunicacao,
        doencas_cronicas, doencas_agudas, medicamentos_controle, fonte, updated_at
    ) AS (
        VALUES
        {values}
    )
    UPDATE clientes_cliente t
    SET
        cod_externo = updated_values.cod_externo,
        eh_pessoa_fisica = updated_values.eh_pessoa_fisica,
        eh_pessoa_juridica = updated_values.eh_pessoa_juridica,
        nome_completo = updated_values.nome_completo,
        data_nascimento = updated_values.data_nascimento,
        sexo = updated_values.sexo,
        email = updated_values.email,
        celular = updated_values.celular,
        telefone_alt = updated_values.telefone_alt,
        cep = updated_values.cep,
        logradouro = updated_values.logradouro,
        numero = updated_values.numero,
        complemento = updated_values.complemento,
        bairro = updated_values.bairro,
        cidade = updated_values.cidade,
        estado = updated_values.estado,
        enderecos = updated_values.enderecos::jsonb,
        autorizacao_comunicacao = updated_values.autorizacao_comunicacao,
        doencas_cronicas = updated_values.doencas_cronicas::text[],
        doencas_agudas = updated_values.doencas_agudas::text[],
        medicamentos_controle = updated_values.medicamentos_controle::text[],
        fonte = updated_values.fonte,
        updated_at = updated_values.updated_at
    FROM updated_values
    WHERE regexp_replace(t.cpf_cnpj, '[^0-9]', '', 'g') = updated_values.cpf_cnpj
    """
    # usa o cpf_cnpj com a mesma formatação para fazer o match

    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_df = df_pandas.iloc[start_idx:end_idx]

        values_list = [create_value_string(row) for _, row in batch_df.iterrows()]
        update_query = update_base_query.format(values=",".join(values_list))
        result = postgres.execute_query(update_query, connection=postgres_connection)

        print(
            f"Processado batch {batch_num + 1}/{num_batches} ({len(values_list)} linhas) - {result.loc[0].status}"
        )


# COMMAND ----------

update_clientes_in_batches(clientes_unicos, postgres, batch_size=1000)
postgres_connection.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê tabela do Copilot para verificação

# COMMAND ----------

postgres.read_table(spark, "clientes_cliente").limit(100).display()
