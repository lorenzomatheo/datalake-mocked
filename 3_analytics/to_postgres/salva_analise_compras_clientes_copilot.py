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
    SparkSession.builder.appName("salva_analise_compras_clientes_copilot")
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

postgres.read_table(spark, "clientes_clientecompras").limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega e prepara os dados dos clientes refinados

# COMMAND ----------

clientes_refinados = spark.read.table(tabela_clientes_conta)
clientes_refinados.limit(100).display()

# COMMAND ----------

# pega as linhas mais atualizadas dos dados de clientes e conta
clientes_contas_unicos = clientes_refinados.orderBy(
    F.desc("atualizado_em")
).dropDuplicates(subset=["cpf_cnpj", "conta"])
clientes_contas_unicos.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executa query de update

# COMMAND ----------


def create_value_string(row):
    return f"""(
        gen_random_uuid(),
        {f"'{escape_string(row.cpf_cnpj)}'" if pd.notna(row.cpf_cnpj) else "''"},
        {f"'{escape_string(row.cliente_conta)}'" if pd.notna(row.cliente_conta) else "''"},
        {f"'{row.data_primeira_compra}'" if pd.notna(row.data_primeira_compra) else 'NULL'},
        {f"'{row.data_ultima_compra}'" if pd.notna(row.data_ultima_compra) else 'NULL'},
        {row.total_de_compras if pd.notna(row.total_de_compras) else '0'},
        {f"string_to_array('{escape_string(row.compras)}', ',')" if pd.notna(row.compras) else 'NULL'},
        {row.ticket_medio if pd.notna(row.ticket_medio) else 'NULL'},
        {row.media_itens_por_cesta if pd.notna(row.media_itens_por_cesta) else 'NULL'},
        {f"'{escape_string(row.pref_dia_semana)}'" if pd.notna(row.pref_dia_semana) else 'NULL'},
        {row.pref_dia_mes if pd.notna(row.pref_dia_mes) else 'NULL'},
        {row.pref_hora if pd.notna(row.pref_hora) else 'NULL'},
        {row.intervalo_medio_entre_compras if pd.notna(row.intervalo_medio_entre_compras) else 'NULL'},
        {f"'{escape_string(row.cluster_rfm)}'" if pd.notna(row.cluster_rfm) else 'NULL'},
        {f"'{escape_string(row.loja_mais_frequente)}'" if pd.notna(row.loja_mais_frequente) else 'NULL'},
        {row.pct_canal_loja_fisica if pd.notna(row.pct_canal_loja_fisica) else 'NULL'},
        {row.pct_canal_tele_entrega if pd.notna(row.pct_canal_tele_entrega) else 'NULL'},
        {row.pct_canal_e_commerce if pd.notna(row.pct_canal_e_commerce) else 'NULL'},
        {row.pct_canal_outros_canais if pd.notna(row.pct_canal_outros_canais) else 'NULL'},
        {f"'{escape_string(row.canal_preferido)}'" if pd.notna(row.canal_preferido) else 'NULL'},
        {row.pct_pgto_cartao if pd.notna(row.pct_pgto_cartao) else 'NULL'},
        {row.pct_pgto_pix if pd.notna(row.pct_pgto_pix) else 'NULL'},
        {row.pct_pgto_dinheiro if pd.notna(row.pct_pgto_dinheiro) else 'NULL'},
        {row.pct_pgto_crediario if pd.notna(row.pct_pgto_crediario) else 'NULL'},
        {f"'{escape_string(row.metodo_pgto_preferido)}'" if pd.notna(row.metodo_pgto_preferido) else 'NULL'},
        {row.pct_produtos_impulsionados if pd.notna(row.pct_produtos_impulsionados) else 'NULL'},
        {row.pct_itens_medicamentos if pd.notna(row.pct_itens_medicamentos) else 'NULL'},
        {row.pct_itens_medicamentos_genericos if pd.notna(row.pct_itens_medicamentos_genericos) else 'NULL'},
        {(1 - row.pct_itens_medicamentos_genericos) if pd.notna(row.pct_itens_medicamentos_genericos) else 'NULL'},
        {f"'{row.criado_em}'::timestamp" if pd.notna(row.criado_em) else 'CURRENT_TIMESTAMP'},
        {f"'{row.atualizado_em}'::timestamp" if pd.notna(row.atualizado_em) else 'CURRENT_TIMESTAMP'}
    )"""


def update_cliente_compras_in_batches(df, postgres, batch_size=1000):
    df_prepared = df.withColumn("compras", F.array_join("compras", ",")).withColumn(
        "cliente_conta", F.col("conta")
    )

    df_pandas = df_prepared.toPandas()
    total_rows = len(df_pandas)
    num_batches = math.ceil(total_rows / batch_size)

    # faz upsert
    update_base_query = """
        WITH updated_values (
            id, cliente_cpf_cnpj, cliente_conta, data_primeira_compra, data_ultima_compra,
            total_de_compras, compras, ticket_medio, media_itens_por_cesta,
            pref_dia_semana, pref_dia_mes, pref_hora, intervalo_medio_entre_compras,
            cluster_rfm, loja_mais_frequente,
            pct_canal_loja_fisica, pct_canal_tele_entrega, pct_canal_e_commerce,
            pct_canal_outros_canais, canal_preferido, pct_pgto_cartao,
            pct_pgto_pix, pct_pgto_dinheiro, pct_pgto_crediario,
            metodo_pgto_preferido, pct_produtos_impulsionados,
            pct_itens_medicamentos, pct_itens_medicamentos_genericos,
            pct_itens_medicamentos_marca, created_at, updated_at
        ) AS (
            SELECT
                v.id,
                v.cliente_cpf_cnpj,
                v.cliente_conta,
                v.data_primeira_compra::date,
                v.data_ultima_compra::date,
                v.total_de_compras::integer,
                v.compras::varchar[],
                v.ticket_medio::double precision,
                v.media_itens_por_cesta::double precision,
                v.pref_dia_semana,
                v.pref_dia_mes::integer,
                v.pref_hora::integer,
                v.intervalo_medio_entre_compras::integer,
                v.cluster_rfm,
                v.loja_mais_frequente,
                v.pct_canal_loja_fisica::double precision,
                v.pct_canal_tele_entrega::double precision,
                v.pct_canal_e_commerce::double precision,
                v.pct_canal_outros_canais::double precision,
                v.canal_preferido,
                v.pct_pgto_cartao::double precision,
                v.pct_pgto_pix::double precision,
                v.pct_pgto_dinheiro::double precision,
                v.pct_pgto_crediario::double precision,
                v.metodo_pgto_preferido,
                v.pct_produtos_impulsionados::double precision,
                v.pct_itens_medicamentos::double precision,
                v.pct_itens_medicamentos_genericos::double precision,
                v.pct_itens_medicamentos_marca::double precision,
                v.created_at,
                v.updated_at
            FROM (VALUES
                {values}
            ) as v (
                id, cliente_cpf_cnpj, cliente_conta, data_primeira_compra, data_ultima_compra,
                total_de_compras, compras, ticket_medio, media_itens_por_cesta,
                pref_dia_semana, pref_dia_mes, pref_hora, intervalo_medio_entre_compras,
                cluster_rfm, loja_mais_frequente,
                pct_canal_loja_fisica, pct_canal_tele_entrega, pct_canal_e_commerce,
                pct_canal_outros_canais, canal_preferido, pct_pgto_cartao,
                pct_pgto_pix, pct_pgto_dinheiro, pct_pgto_crediario,
                metodo_pgto_preferido, pct_produtos_impulsionados,
                pct_itens_medicamentos, pct_itens_medicamentos_genericos,
                pct_itens_medicamentos_marca, created_at, updated_at
            )
        )
        INSERT INTO clientes_clientecompras (
            id, cliente_cpf_cnpj, cliente_conta, data_primeira_compra, data_ultima_compra,
            total_de_compras, compras, ticket_medio, media_itens_por_cesta,
            pref_dia_semana, pref_dia_mes, pref_hora, intervalo_medio_entre_compras,
            cluster_rfm, loja_mais_frequente,
            pct_canal_loja_fisica, pct_canal_tele_entrega, pct_canal_e_commerce,
            pct_canal_outros_canais, canal_preferido, pct_pgto_cartao,
            pct_pgto_pix, pct_pgto_dinheiro, pct_pgto_crediario,
            metodo_pgto_preferido, pct_produtos_impulsionados,
            pct_itens_medicamentos, pct_itens_medicamentos_genericos,
            pct_itens_medicamentos_marca, created_at, updated_at
        )
        SELECT * FROM updated_values
        ON CONFLICT (cliente_cpf_cnpj, cliente_conta)
        DO UPDATE SET
            data_primeira_compra = EXCLUDED.data_primeira_compra,
            data_ultima_compra = EXCLUDED.data_ultima_compra,
            total_de_compras = EXCLUDED.total_de_compras,
            compras = EXCLUDED.compras,
            ticket_medio = EXCLUDED.ticket_medio,
            media_itens_por_cesta = EXCLUDED.media_itens_por_cesta,
            pref_dia_semana = EXCLUDED.pref_dia_semana,
            pref_dia_mes = EXCLUDED.pref_dia_mes,
            pref_hora = EXCLUDED.pref_hora,
            intervalo_medio_entre_compras = EXCLUDED.intervalo_medio_entre_compras,
            cluster_rfm = EXCLUDED.cluster_rfm,
            loja_mais_frequente = EXCLUDED.loja_mais_frequente,
            pct_canal_loja_fisica = EXCLUDED.pct_canal_loja_fisica,
            pct_canal_tele_entrega = EXCLUDED.pct_canal_tele_entrega,
            pct_canal_e_commerce = EXCLUDED.pct_canal_e_commerce,
            pct_canal_outros_canais = EXCLUDED.pct_canal_outros_canais,
            canal_preferido = EXCLUDED.canal_preferido,
            pct_pgto_cartao = EXCLUDED.pct_pgto_cartao,
            pct_pgto_pix = EXCLUDED.pct_pgto_pix,
            pct_pgto_dinheiro = EXCLUDED.pct_pgto_dinheiro,
            pct_pgto_crediario = EXCLUDED.pct_pgto_crediario,
            metodo_pgto_preferido = EXCLUDED.metodo_pgto_preferido,
            pct_produtos_impulsionados = EXCLUDED.pct_produtos_impulsionados,
            pct_itens_medicamentos = EXCLUDED.pct_itens_medicamentos,
            pct_itens_medicamentos_genericos = EXCLUDED.pct_itens_medicamentos_genericos,
            pct_itens_medicamentos_marca = EXCLUDED.pct_itens_medicamentos_marca,
            updated_at = CURRENT_TIMESTAMP
        """

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


update_cliente_compras_in_batches(clientes_contas_unicos, postgres, batch_size=1000)
postgres_connection.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê tabela do Copilot para verificação

# COMMAND ----------

postgres.read_table(spark, "clientes_clientecompras").limit(100).display()
