# Databricks notebook source
# MAGIC %pip install gspread google-auth

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import json

import gspread
import pyspark.sql.functions as F
from google.oauth2.service_account import Credentials
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.io.postgres import PostgresAdapter
from maggulake.schemas import schema_sftp_tradefy

# COMMAND ----------

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("adiciona_correcoes_manuais")
    .config(conf=config)
    .getOrCreate()
)

# ambiente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(
    stage, POSTGRES_USER, POSTGRES_PASSWORD, utilizar_read_replica=True
)

# google sheets credentials
json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
SHEET_ID = "1pllSEsUe-74UULG94avG5nSDQSwl2KPKNHGnkXx9w7s"
aba_opt_in_pix_de_volta = "opt_in_pix_de_volta"
aba_info_pix_de_volta = "info_pix_de_volta"
aba_eans = 'lista_produtos'

# aws
aws_tradefy_path = 's3://maggu-datalake-prod/1-raw-layer/tradefy/'

# delta
apuracao_pix_de_volta_table = 'hive_metastore.pbi.apuracao_cashback_pixdevolta'

# COMMAND ----------

creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)
gc = gspread.authorize(creds)

# planilha cashback pix de volta
rows_aba_pix_de_volta = (
    gc.open_by_key(SHEET_ID).worksheet(aba_opt_in_pix_de_volta).get_all_records()
)
rows_info_pix_de_volta = (
    gc.open_by_key(SHEET_ID).worksheet(aba_info_pix_de_volta).get_all_records()
)
df_opt_in_pix_de_volta = spark.createDataFrame(rows_aba_pix_de_volta)
df_info_pix_de_volta = spark.createDataFrame(rows_info_pix_de_volta)

# eans disponiveis para cashback pix de volta
rows_eans = gc.open_by_key(SHEET_ID).worksheet(aba_eans).get_all_records()

# COMMAND ----------

# Define o escopo de eans disponiveis e redes que deram opt_in
redes_para_desconsiderar = [
    'farmacias_exclusivas'
]  # por enquanto somente essa por ser uma distribuidora e tem um fluxo diferente

lista_eans = list(set(str(row['ean']) for row in rows_eans))
lista_redes = list(
    str(row['rede'])
    for row in rows_aba_pix_de_volta
    if row['rede'] not in redes_para_desconsiderar
)

# COMMAND ----------

# Informações adicionais
df_info_conta = (
    postgres.read_table(spark, 'contas_loja')
    .filter(F.col("databricks_tenant_name").isin(lista_redes))
    .select(
        F.col('id').alias('loja_id'),
        F.col('cnpj').alias("cnpj_loja"),
        F.col('databricks_tenant_name').alias('conta_loja'),
    )
)

df_info_produto = (
    spark.read.table("production.refined.produtos_refined")
    .filter(F.col('ean').isin(lista_eans))
    .select('ean', 'nome', 'marca', 'fabricante')
)

# COMMAND ----------

df_periodo_apuracao = (
    df_opt_in_pix_de_volta.withColumn('today', F.current_date())
    .withColumn('diff', F.date_diff(F.col("data_fim"), F.col('today')))
    .groupBy('data_inicio', 'data_fim')
    .agg(F.min("diff").alias("diff"))
    .drop('diff')
)

# COMMAND ----------

inicio_apuracao = (
    df_periodo_apuracao.select('data_inicio').first().asDict()['data_inicio']
)
fim_apuracao = df_periodo_apuracao.select('data_fim').first().asDict()['data_fim']

print(f'Periodo de apuração: {inicio_apuracao} a {fim_apuracao}')

# COMMAND ----------

df_notas_tradefy = (
    spark.read.csv(aws_tradefy_path, sep="|", header=True, schema=schema_sftp_tradefy)
    .dropDuplicates()  # importante remover duplicatas pq os arquivos são incrementais em uma janela rolante
    .select(
        F.col('CNPJ_Filial_Distribuidor').alias('cnpj_distribuidor'),
        F.col('CNPJ_PDV').alias('cnpj_loja'),
        F.col('Cod_Prod').alias('ean'),
        F.col('Data_Nota_Fiscal').alias('data_nota_fiscal'),
        F.col('Numero_Nota_Fiscal').alias('numero_nota_fiscal'),
        F.col('Tipo_Documento').alias('tipo_documento'),
        F.col('Venda_Unidades').alias('venda_unidades'),
        F.col('Venda_Liquida').alias('venda_liquida'),
    )
    .join(
        df_info_conta, how='inner', on='cnpj_loja'
    )  # somente redes com opt in na planilha
    .filter(F.col('tipo_documento') == 'F')  # somente notas faturadas
    .withColumn(
        "data_nota_fiscal",
        F.date_format(F.to_date(F.col('data_nota_fiscal'), "yyyyMMdd"), "yyyy-MM-dd"),
    )
)

# COMMAND ----------

# # Leitura de notas fiscais já apuradas da campanha pix_de_volta
df_notas_fiscais_apuradas = (
    spark.read.table(apuracao_pix_de_volta_table)
    .filter(
        F.col('tipo_campanha')
        == F.lit(f'pix_de_volta - {inicio_apuracao}/{fim_apuracao}')
    )  # verificar somente notas da campanha vigente
    .withColumn('notas_fiscais', F.explode('notas_fiscais'))
    .groupBy(
        'conta_loja',
    )
    .agg(F.collect_set('notas_fiscais').alias('notas_fiscais'))
)

# COMMAND ----------

df_notas_nao_apuradas = (
    df_notas_tradefy.join(df_notas_fiscais_apuradas, how='inner', on='conta_loja')
    .withColumn(
        "check_nota_fiscal",
        F.array_contains(F.col('notas_fiscais'), F.col('numero_nota_fiscal')),
    )
    .filter(F.col("check_nota_fiscal") == False)  # somente novas notas fiscais
)

# COMMAND ----------

# calcula cashback no periodo de vigência da campanha
df_apuracao_pix_de_volta = (
    df_notas_nao_apuradas.filter(
        (F.col('data_nota_fiscal') >= inicio_apuracao)
        & (F.col('data_nota_fiscal') <= fim_apuracao)
    )
    .groupBy("conta_loja", "ean", "data_nota_fiscal")
    .agg(
        F.sum("venda_unidades").alias("quantidade_comprada_intervalo_apuracao"),
        F.round(F.sum("venda_liquida"), 2).alias("valor_total_compra"),
        F.collect_list('numero_nota_fiscal').alias('notas_fiscais'),
    )
    .join(df_info_pix_de_volta, how='inner', on=['ean'])
    .withColumn(
        "valor_cashback",
        F.round(F.col('valor_total_compra') * (F.col('desconto') / 100), 2),
    )
    .withColumn(
        "tipo_campanha", F.lit(f'pix_de_volta - {inicio_apuracao}/{fim_apuracao}')
    )
    .withColumn("data_execucao_apuracao", F.lit(F.current_date()))
)

# COMMAND ----------

df_apuracao_pix_de_volta.write.mode("append").saveAsTable(apuracao_pix_de_volta_table)
