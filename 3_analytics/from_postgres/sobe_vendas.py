# Databricks notebook source
# MAGIC %pip install pandas gspread gspread-dataframe google PyDrive2 cryptography==41.0.7 pyOpenSSL==23.2.0 tiktoken
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])
dbutils.widgets.dropdown("debug", "false", ["false", "true"])
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"], "Full Refresh")

# COMMAND ----------

from datetime import datetime

import pandas as pd
import psycopg2
import pytz
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# Spark

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])
spark = SparkSession.builder.appName("sobe_vendas").config(conf=config).getOrCreate()

# Delta Lake

DEBUG = dbutils.widgets.get("debug")
stage = dbutils.widgets.get("stage")
full_refresh = dbutils.widgets.get("full_refresh") == "true"
catalog = "production" if stage == "prod" else "staging"
tabela_vendas_path = f"{catalog}.analytics.view_vendas"

# Postgres

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(stage, USER, PASSWORD)

analysis_start_date = '2020-01-01'  # data default para full refresh
write_mode = "overwrite"
existing_venda_ids = set()

if not full_refresh:
    max_date_result = spark.sql(f"""
        SELECT MAX(DATE(realizada_em)) as max_date
        FROM {tabela_vendas_path}
    """).collect()

    if max_date_result and max_date_result[0]['max_date']:
        max_date = max_date_result[0]['max_date']
        # pega dados de 1 dia antes para ter overlap e evitar perda de dados
        analysis_start_date = (max_date - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        write_mode = "append"

        # Pega os ids de venda já processados para evitar duplicação
        existing_df = spark.sql(f"""
            SELECT DISTINCT venda_id
            FROM {tabela_vendas_path}
            WHERE DATE(realizada_em) >= '{analysis_start_date}'
        """)
        existing_venda_ids = {row.venda_id for row in existing_df.collect()}

        print(f"Processando dados a partir de {analysis_start_date}")
    else:
        print("Tabela vazia, executando FULL REFRESH")

else:
    print("Executando FULL REFRESH")

# COMMAND ----------

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 1000)


def agora():
    return datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S")


# COMMAND ----------

try:
    conn = psycopg2.connect(
        database=postgres.DATABASE_NAME,
        user=postgres.USER,
        host=postgres.DATABASE_HOST,
        password=postgres.PASSWORD,
        port=postgres.DATABASE_PORT,
    )
    cursor = conn.cursor()

    query = f"""
        SELECT
            vv.id AS venda_id,
            vv.pre_venda_id,
            vv.codigo_externo AS codigo_externo_venda,
            vv.codigo_externo_da_pre_venda AS codigo_externo_pre_venda,
            vv.codigo_externo_do_vendedor AS codigo_externo_vendedor,
            vv.cpf_cnpj_do_cliente AS cpf_cnpj_cliente,
            vv.realizada_em,
            vv.criado_em AS venda_criado_em,
            vv.atualizado_em AS venda_atualizado_em,
            vv.vendedor_id,

            vi.id AS venda_item_id,
            vi.ean AS produto_ean,
            vi.preco_venda * vi.quantidade AS preco_venda,
            vi.preco_venda_desconto * vi.quantidade AS preco_venda_desconto,
            vi.custo_compra AS custo_compra,
            vi.quantidade AS quantidade,

            vi.produto_id AS produto_id,
            ppv2.nome AS nome_produto,
            ppv2.fabricante AS marca_produto,
            ppv2.descricao AS descricao_produto,
            ppv2.eh_tarjado AS eh_tarjado_produto,
            ppv2.eh_medicamento AS eh_medicamento_produto,
            ppv2.eh_controlado AS eh_controlado_produto,
            ppv2.categorias AS categorias_produto,
            ppv2.forma_farmaceutica AS forma_farmaceutica_produto,
            ppv2.tamanho_produto AS tamanho_produto,

            vv.cliente_id,
            cc.nome_social AS nome_social_cliente,
            cc.cpf_cnpj AS cpf_cliente,
            cc.sexo AS sexo_cliente,

            vv.loja_id,
            cl.databricks_tenant_name AS conta_loja,
            cl.name AS nome_loja,
            cl.codigo_loja,

            aa.username AS username_vendedor

        FROM
            vendas_item vi
        JOIN
            vendas_venda vv ON vi.venda_id = vv.id
        LEFT JOIN
            produtos_produtov2 ppv2 ON vi.produto_id = ppv2.id
        LEFT JOIN
            contas_loja cl ON vv.loja_id = cl.id
        LEFT JOIN
            clientes_cliente cc ON vv.cliente_id = cc.id
        LEFT JOIN
            atendentes_atendente aa on vv.vendedor_id = aa.usuario_django_id
        WHERE vv.status like 'venda-concluida'
            AND vi.status like 'vendido'
            AND DATE(vv.realizada_em) >= '{analysis_start_date}'
    """

    count_query = f"SELECT COUNT(*) FROM ({query}) as sub_query"
    cursor.execute(count_query)
    total_rows = cursor.fetchone()[0]

    if total_rows == 0:
        print("Nenhum novo dado encontrado.")
        cursor.close()
        conn.close()
        dbutils.notebook.exit("Nenhum novo dado encontrado.")

    cursor.execute(query)
    chunk_size = 10000
    processed_rows = 0
    temp_path = "/tmp/maggu/view_vendas_temp"

    while True:
        data = cursor.fetchmany(chunk_size)
        if not data:
            break

        processed_rows += len(data)
        columns = [desc[0] for desc in cursor.description]
        chunk_df = pd.DataFrame(data, columns=columns)

        # Filtra venda_ids para evitar duplicação
        if existing_venda_ids:
            initial_count = len(chunk_df)
            chunk_df = chunk_df[~chunk_df['venda_id'].isin(existing_venda_ids)]
            filtered_count = len(chunk_df)
            if DEBUG == "true":
                print(
                    f"Filtrados {initial_count - filtered_count} registros duplicados"
                )

        # Pula vazios
        if len(chunk_df) == 0:
            print(f"{agora()} - Chunk vazio após filtro de duplicatas, pulando...")
            continue

        chunk_df["preco_venda"] = chunk_df["preco_venda"].astype(float)
        chunk_df["preco_venda_desconto"] = chunk_df["preco_venda_desconto"].astype(
            float
        )
        chunk_df["custo_compra"] = chunk_df["custo_compra"].astype(float)
        chunk_df["quantidade"] = chunk_df["quantidade"].astype(float)
        chunk_spark_df = spark.createDataFrame(chunk_df)

        temp_write_mode = "overwrite" if processed_rows <= chunk_size else "append"
        chunk_spark_df.write.mode(temp_write_mode).parquet(temp_path)

        print(
            f"{agora()} - Sucesso, {processed_rows}/{total_rows} registros foram processados!"
        )

    if DEBUG == "true":
        display(spark.read.parquet(temp_path))

    final_df = spark.read.parquet(temp_path)

    if not final_df.take(1):
        print("Dataframe vazio após filtros, parando execução.")
        dbutils.notebook.exit("Dataframe vazio após filtros.")

    final_df.write.mode(write_mode).saveAsTable(tabela_vendas_path)

    print(
        f"Dados salvos com sucesso em {tabela_vendas_path} usando modo '{write_mode}'"
    )

    conn.commit()
except psycopg2.DatabaseError as error:
    print(f"An error occurred: {error}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
