# Databricks notebook source
# MAGIC %pip install pandas gspread gspread-dataframe google PyDrive2 cryptography==41.0.7 pyOpenSSL==23.2.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import csv
import json
import re

import pandas as pd
import psycopg2
from oauth2client.service_account import ServiceAccountCredentials
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

config = SparkConf().setAll(pairs=[("spark.sql.caseSensitive", "true")])

spark = SparkSession.builder.appName("Cesta Compras").config(conf=config).getOrCreate()

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")

GOOGLE_SHEETS_SERVICE_ACCOUNT_INFO = json.loads(
    dbutils.secrets.get(scope="databricks", key="GOOGLE_SHEETS_SERVICE_ACCOUNT_INFO")
)

# Instancie o objeto GoogleAuth
gauth = GoogleAuth()

# Carregue as credenciais do arquivo de conta de serviço
scope = ['https://www.googleapis.com/auth/drive']  # O escopo que você precisa
credenciais = ServiceAccountCredentials.from_json_keyfile_dict(
    GOOGLE_SHEETS_SERVICE_ACCOUNT_INFO, scope
)

# Atribua as credenciais ao GoogleAuth
gauth.credentials = credenciais

# COMMAND ----------

# MAGIC %md
# MAGIC Listando tabelas disponíveis

# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD)

postgres.list_tables()

# COMMAND ----------

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 1000)

df = pd.DataFrame()

try:
    conn = psycopg2.connect(
        database=postgres.DATABASE_NAME,
        user=postgres.USER,
        host=postgres.DATABASE_HOST,
        password=postgres.PASSWORD,
        port=postgres.DATABASE_PORT,
    )
    cursor = conn.cursor()

    query = """
    SELECT

        cdc.id as basket_id,
        cdc.aberta_em AS opened_at,
        cdc.fechada_em AS closed_at,
        COALESCE(cdc.status,'N/A') AS status,
        cdc.tipo_entrega AS sale_channel,
        cdc.forma_pagamento AS payment_method,

        cci.produto_id AS product_id,
        ppv2.ean AS product_ean,
        ppv2.nome AS product_name,
        ppv2.fabricante AS brand,
        ppv2.descricao AS description,
        ppv2.eh_medicamento AS is_medicine,
        ppv2.categorias AS categories,
        cci.produto_substituido_id AS produto_substituido_id,

        cci.preco_venda AS sale_price,
        cci.preco_venda_desconto AS discount_price,
        cci.usa_pbm AS usa_pbm,
        cci.custo_compra AS cost_price,
        cci.quantidade AS quantity,
        cci.origem AS origin,
        cci.tipo_recomendacao AS tipo_recomendacao,
        cci.tipo_venda AS tipo_venda,
        cci.tipo_comissao AS tipo_comissao,
        cci.maggu_coins AS maggu_coins,

        cdc.cliente_id AS customer_id,
        cc.nome_social,
        cci.cpf AS customer_cpf,
        cci.idade AS customer_age,
        cci.sexo AS customer_gender,
        cci.sintomas AS symptoms,

        cl.id AS store_id,
        cl.name AS store_name,
        cl.codigo_loja AS store_code,

        au.id AS user_id,
        au.username AS username

    FROM
        cesta_de_compras_item cci
    JOIN
        cesta_de_compras_cesta cdc ON cci.cesta_id = cdc.id
    JOIN
        produtos_produtov2 ppv2 ON cci.produto_id = ppv2.id
    JOIN
        contas_loja cl ON cdc.loja_id = cl.id
    JOIN
        auth_user au ON cdc.user_id = au.id
    LEFT JOIN
        clientes_cliente cc ON cdc.cliente_id = cc.id
  """

    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(data, columns=columns)

    # pra garantir que não vai dar conflito entre colunas
    def rename_duplicates(old_columns):
        seen = {}
        for idx, column in enumerate(old_columns):
            if column in seen:
                seen[column] += 1
                old_columns[idx] = f"{column}_{seen[column]}"
            else:
                seen[column] = 0
        return old_columns

    df.columns = rename_duplicates(list(df.columns))

    display(df)
    conn.commit()
except psycopg2.DatabaseError as error:
    print(f"An error occurred: {error}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()

# COMMAND ----------

if not df.empty:
    # salva no databricks
    spark_df = spark.createDataFrame(df)
    spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        f"{catalog}.analytics.view_cestas_de_compras"
    )

    # salva no google drive
    def concat_categories(x):
        if x:
            return '|'.join(x)
        else:
            return ''

    def remove_special_characters(x):
        # Remove todos os caracteres que não sejam alfanuméricos
        x = re.sub(r'[^a-zA-Z0-9\s\.\-\_]', '', x)

        # Substitui espaços em branco por um único espaço
        x = re.sub(r'\s+', ' ', x)

        return x.strip()

    def update_or_create_file(drive, file_name, folder_id, content):
        # Busca arquivo
        file_list = drive.ListFile(
            {'q': f"title='{file_name}' and '{folder_id}' in parents and trashed=false"}
        ).GetList()

        # Seleciona arquivo ou cria novo
        file = (
            file_list[0]
            if file_list
            else drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}]})
        )

        # Seta novo conteúdo e faz upload
        file.SetContentString(content)
        file.Upload()

        print(
            f"Arquivo '{file_name}' foi {'atualizado' if file_list else 'criado'} com sucesso!"
        )

    df_cestas = df.copy()
    df_cestas = df_cestas.drop(columns=['description'])
    df_cestas['opened_at'] = df_cestas['opened_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_cestas['closed_at'] = df_cestas['closed_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_cestas = df_cestas.replace([float('inf'), float('-inf')], None)
    df_cestas = df_cestas.fillna(0)
    df_cestas['categories'] = df_cestas['categories'].apply(
        lambda x: concat_categories(x)
    )
    df_cestas = df_cestas.astype(str)
    df_cestas = df_cestas.applymap(remove_special_characters)

    drive = GoogleDrive(gauth)
    file_name = 'cestas.csv'
    folder_id = '1js0ykl_enIE2oq7AynXZ_0jIO9KOfMVc'
    content = df_cestas.to_csv(index=False, quoting=csv.QUOTE_ALL)

    update_or_create_file(drive, file_name, folder_id, content)
