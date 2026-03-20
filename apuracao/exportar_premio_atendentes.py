# Databricks notebook source
# MAGIC %md
# MAGIC ### Apuração do pagamento dos atendentes
# MAGIC Este script realiza o cálculo da apuração dos atendentes por tipo de missão (Maggu ou interna do dono da farmácia) e salva os resultados em duas planilhas no Sheets, para que o time financeiro possa efetuar o pagamento e controlar quais atendentes já foram pagos.
# MAGIC
# MAGIC O cálculo da apuração utiliza como tabela principal a tabela do Postgres gameficacao_extratodemoedas, pois é a única que contém as "moedas" por missão.

# COMMAND ----------

# MAGIC %md
# MAGIC # Importações e parâmetros utilizados ao longo do script

# COMMAND ----------

# MAGIC %pip install gspread google-auth

# COMMAND ----------

import json
from datetime import date, datetime

import gspread
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from pyspark.context import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string

from maggulake.environment import CopilotTable, DatabricksEnvironmentBuilder
from maggulake.io.postgres import PostgresAdapter

# COMMAND ----------

# sheets que o financeiro utiliza para fazer os pagamentos
SHEET_ID = "1BYORNX0NWCccsJT7XPTAfyydd3qrYBHxV2hsDcU2y-c"
SHEET_NAME_MISSAO_MAGGU = "apuracao_missao_maggu"
SHEET_NAME_MISSAO_INTERNA = "apuracao_missao_interna"

env = DatabricksEnvironmentBuilder.build(
    "apuracao_atendentes",
    dbutils,
)

# COMMAND ----------

# pega as ultimas 2 semanas
data_inicio_missao = (
    spark.range(1)
    .select(
        F.date_format(
            F.date_sub(F.next_day(F.current_date(), "Mon"), 21), "yyyy-MM-dd"
        ).alias("data_inicio_missao")
    )
    .first()["data_inicio_missao"]
)

print(data_inicio_missao)

# COMMAND ----------

# pega a data inicio da rodada retrasada
data_inicio_missao_passada = (
    spark.range(1)
    .select(
        F.date_format(
            F.date_sub(F.next_day(F.current_date(), "Mon"), 21), "yyyy-MM-dd"
        ).alias("data_inicio_missao")
    )
    .first()["data_inicio_missao"]
)

print(data_inicio_missao_passada)

# COMMAND ----------

# pega a data inicio da rodada passada
data_inicio_missao_atual = (
    spark.range(1)
    .select(
        F.date_format(
            F.date_sub(F.next_day(F.current_date(), "Mon"), 14), "yyyy-MM-dd"
        ).alias("data_inicio_missao")
    )
    .first()["data_inicio_missao"]
)

print(data_inicio_missao_atual)

# COMMAND ----------

config = SparkConf().setAll(
    [
        ("spark.sql.caseSensitive", "true"),
        ("spark.sql.session.timeZone", "America/Sao_Paulo"),
    ]
)

spark = SparkSession.builder.appName("Apuração").config(conf=config).getOrCreate()

stage = 'prod'

catalog = "production"

USER = dbutils.secrets.get(scope="postgres", key="USER_APP")
PASSWORD = dbutils.secrets.get(scope="postgres", key="PASSWORD_APP")

# o ideal é pegar os dados do postgres e não do BQ para pegar dados mais atualizados possível
postgres = PostgresAdapter(stage, USER, PASSWORD, utilizar_read_replica=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções auxiliares

# COMMAND ----------


def get_google_credentials() -> Credentials:
    try:
        credentials_json = dbutils.secrets.get(
            scope="databricks", key="GOOGLE_SHEETS_PBI"
        )
        credentials_dict = json.loads(credentials_json)

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive',
        ]

        credentials = Credentials.from_service_account_info(
            credentials_dict, scopes=scopes
        )
        return credentials
    except Exception as e:
        raise RuntimeError(f"Erro ao obter credenciais do Google: {str(e)}")


def salvar_resultados_google_sheets(
    df_resultado, sheet_id: str, worksheet_name: str, credentials
):
    sheet_id = str(sheet_id)
    worksheet_name = str(worksheet_name)

    try:
        # Spark -> pandas
        df_pandas = (
            df_resultado.toPandas().replace([np.inf, -np.inf], np.nan).fillna("")
        )

        if df_pandas.empty:
            print("Nenhum resultado para salvar")
            return

        # Normaliza datas para strings
        for col in df_pandas.columns:
            if pd.api.types.is_datetime64_any_dtype(df_pandas[col]):
                df_pandas[col] = df_pandas[col].dt.strftime('%Y-%m-%d')
        df_pandas = df_pandas.apply(
            np.vectorize(
                lambda x: (
                    x.strftime('%Y-%m-%d')
                    if isinstance(x, (pd.Timestamp, datetime, date))
                    else x
                )
            )
        )

        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sheet_id)

        try:
            worksheet = sheet.worksheet(worksheet_name)
            dados_existentes = worksheet.get_all_records()
            df_existente = pd.DataFrame(dados_existentes)
            df_completo = pd.concat([df_existente, df_pandas], ignore_index=True)
        except WorksheetNotFound:
            rows = int(max(1000, len(df_pandas) + 1))
            cols = int(max(13, len(df_pandas.columns)))
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)
            df_completo = df_pandas

        df_completo = df_completo.fillna("")

        # Tudo como string para evitar erro de serialização
        headers = list(map(str, df_completo.columns))
        values = [headers] + (
            df_completo.astype(object)
            .where(pd.notna(df_completo), "")
            .astype(str)
            .replace("NaT", "")
            .values.tolist()
        )

        worksheet.clear()
        worksheet.update(values, 'A1')

    except Exception as e:
        raise RuntimeError(f"Erro ao salvar resultados: {str(e)}")


# COMMAND ----------

credentials = get_google_credentials()

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga dos dados do BQ

# COMMAND ----------

# a tabela abaixo tem a tabela de vendas como uma das origens, por isso o melhor é consultar do BQ
df_user_loja = env.table(CopilotTable.atendente_loja_apuracao_atendente)

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga dos dados - Postgres

# COMMAND ----------

df_contas_loja = postgres.read_table(spark, "contas_loja")

# COMMAND ----------

aa = postgres.read_table(spark, "atendentes_atendente").alias("aa")
ai = postgres.read_table(spark, "atendentes_informacoespessoais").alias("ai")

df_user_joined = (
    aa.join(ai, aa.id == ai.atendente_id, how="inner")
    .drop(aa.id)
    .drop(ai.id)
    .withColumnRenamed("atendente_id", "id")
    .withColumn(
        "username", F.lower(F.regexp_replace(F.col("aa.username"), r"^\s+|\s+$", ""))
    )
    .withColumn(
        "nome",
        F.when(F.col("ai.nome") == "", F.col("aa.first_name")).otherwise(
            F.col("ai.nome")
        ),
    )
    .select("id", "username", "nome", "aa.email", "ai.cpf", "ai.telefone")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo do prêmio - extratodemoedas

# COMMAND ----------

df_missao = postgres.read_table(spark, 'gameficacao_missao')

# COMMAND ----------

df_extrato_moedas = postgres.read_table(spark, 'gameficacao_extratodemoedas')

# COMMAND ----------

df_rodada = postgres.read_table(spark, 'gameficacao_rodada')

# COMMAND ----------

df_join_missao_saldo = (
    df_extrato_moedas.join(
        df_missao, df_extrato_moedas.missao_id == df_missao.id, how="inner"
    )
    .join(df_rodada, df_extrato_moedas.rodada_id == df_rodada.id, how="inner")
    .withColumn("semana_do_ano", F.weekofyear(df_rodada.data_inicio))
    .groupby(
        df_rodada.data_inicio,
        df_missao.id.alias('missao_id_missao'),
        df_missao.nome.alias('nome_missao'),
        'semana_do_ano',
        'missao_id',
        'atendente_id',
    )
    .agg(F.sum('moedas').alias('moedas_acumuladas'))
    .select(
        df_rodada.data_inicio.alias('data_inicio_missao'),
        'semana_do_ano',
        'atendente_id',
        'nome_missao',
        'missao_id_missao',
        (F.col('moedas_acumuladas') / 100).alias('premio_valor'),
    )
)

# COMMAND ----------

# filtra apenas as duas ultimas rodadas
df_premio_missao = df_join_missao_saldo.filter(
    F.col('data_inicio_missao') >= data_inicio_missao_passada
).filter(F.col('premio_valor') > 0)

# COMMAND ----------

df_premio_missao = df_premio_missao.join(
    df_user_joined, df_premio_missao.atendente_id == df_user_joined.id, how="left"
).select(
    'data_inicio_missao',
    'semana_do_ano',
    'atendente_id',
    'username',
    'nome_missao',
    'missao_id_missao',
    'premio_valor',
)

# COMMAND ----------

# Checagem do valor total a ser pago direto do banco
print(
    f'Valor total a pagar da rodada passada: {df_premio_missao.filter(F.col("data_inicio_missao") == data_inicio_missao_atual).agg(F.sum("premio_valor")).collect()[0][0]}'
)

print(
    f'Valor total a pagar da rodada retrasada: {(df_premio_missao.filter(F.col("data_inicio_missao") == data_inicio_missao_passada).agg(F.sum("premio_valor")).collect()[0][0])}'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Join prêmio & cadastro dos atendentes

# COMMAND ----------

df_premio_individual = (
    df_premio_missao.join(
        df_user_joined,
        df_premio_missao["atendente_id"] == df_user_joined["id"],
        "left",
    )
    .join(
        df_user_loja,
        (df_premio_missao['atendente_id'] == df_user_loja['atendente_id'])
        & (df_premio_missao['data_inicio_missao'] == df_user_loja['data_inicio']),
        how="left",
    )
    .groupby(
        F.col('data_inicio_missao').alias('data_inicio'),
        df_premio_missao['username'],
        F.col('nome').alias('nome_completo'),
        'email',
        'cpf',
        'telefone',
        'loja_id',
        'nome_missao',
        'missao_id_missao',
    )
    .agg(F.sum('premio_valor').alias('premio_valor'))
)

# COMMAND ----------

# Checagem do valor total a ser pago após o join com a tabela de atendentes
print(
    f'Valor total a pagar da rodada passada: {df_premio_individual.filter(F.col("data_inicio") == data_inicio_missao_atual).agg(F.sum("premio_valor")).collect()[0][0]}'
)

print(
    f'Valor total a pagar da rodada retrasada: {(df_premio_individual.filter(F.col("data_inicio") == data_inicio_missao_passada).agg(F.sum("premio_valor")).collect()[0][0])}'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apuração da rodada passada

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apuração missões maggu

# COMMAND ----------

df_premio_individual_maggu = (
    df_premio_individual.filter(
        ~F.col('nome_missao').like('Interna%')
    )  # desconsiderar missões internas do dono
    .join(
        df_contas_loja, df_premio_individual["loja_id"] == df_contas_loja["id"], "left"
    )
    .drop(df_contas_loja["telefone"])
    .withColumn('semana_do_ano', F.weekofyear(F.col('data_inicio')))
    .withColumn('tipo_premio', F.lit('missao-maggu'))
    # transformação necessária para garantir que esses campos não sejam tratados como números no sheets
    .withColumn("cpf", format_string('"%s"', "cpf"))
    .withColumn("cnpj", format_string('"%s"', "cnpj"))
    .withColumn("telefone", format_string('"%s"', "telefone"))
    .groupby(
        'data_inicio',
        'semana_do_ano',
        'cnpj',
        F.col('databricks_tenant_name').alias('rede'),
        F.col('name').alias('nome_loja'),
        'username',
        'tipo_premio',
        'cpf',
        'nome_completo',
        'email',
        'telefone',
    )
    .agg(F.sum('premio_valor').cast('int').alias('premio_valor'))
    # colunas de apoio para o time de suporte e financeiro
    .withColumn(
        "premio_ajustado_financeiro", F.lit('')
    )  # campo vazio apenas para que o time do financeiro possa registrar valores extra apuração que foram pagos
    .withColumn("status_financeiro", F.lit('pagamento pendente'))
    .withColumn("motivo_status", F.lit(''))
    .orderBy("rede", "nome_loja")
)

# COMMAND ----------

df_premio_individual_maggu = df_premio_individual_maggu.dropDuplicates(
    ["username", "semana_do_ano"]
)

# COMMAND ----------

# Validação da quantidade de linhas e colunas a serem salvas
(df_premio_individual_maggu.count(), len(df_premio_individual_maggu.columns))

# COMMAND ----------

# Checagem do valor total a ser pago após a separação de missao maggu
print(
    f'Valor total a pagar da rodada passada: {df_premio_individual_maggu.filter(F.col("data_inicio") == data_inicio_missao_atual).agg(F.sum("premio_valor")).collect()[0][0]}'
)

print(
    f'Valor total a pagar da rodada retrasada: {(df_premio_individual_maggu.filter(F.col("data_inicio") == data_inicio_missao_passada).agg(F.sum("premio_valor")).collect()[0][0])}'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apuração missões internas das farmácias

# COMMAND ----------

df_premio_individual_dono = (
    df_premio_individual.filter(
        F.col('nome_missao').like('Interna%')
    )  # considerar apenas missões internas do dono
    .join(
        df_contas_loja, df_premio_individual["loja_id"] == df_contas_loja["id"], "left"
    )
    .drop(df_contas_loja["telefone"])
    .withColumn('semana_do_ano', F.weekofyear(F.col('data_inicio')))
    .withColumn('tipo_premio', F.lit('missao-interna-dono'))
    # transformação necessária para garantir que esses campos não sejam tratados como números no sheets
    .withColumn("cpf", format_string('"%s"', "cpf"))
    .withColumn("cnpj", format_string('"%s"', "cnpj"))
    .withColumn("telefone", format_string('"%s"', "telefone"))
    .groupby(
        'data_inicio',
        'semana_do_ano',
        'cnpj',
        F.col('databricks_tenant_name').alias('rede'),
        F.col('name').alias('nome_loja'),
        'username',
        'tipo_premio',
        'cpf',
        'nome_completo',
        'email',
        'telefone',
    )
    .agg(F.sum('premio_valor').cast('int').alias('premio_valor'))
    .withColumn(
        "premio_ajustado_financeiro", F.lit('')
    )  # campo vazio apenas para que o time do financeiro possa registrar valores extra apuração que foram pagos
    .withColumn("status_financeiro", F.lit('pagamento pendente'))
    .withColumn("motivo_status", F.lit(''))
    .orderBy("rede", "nome_loja")
)

# COMMAND ----------

df_premio_individual_dono = df_premio_individual_dono.dropDuplicates(
    ["username", "semana_do_ano"]
)

# COMMAND ----------

# Validação da quantidade de linhas e colunas a serem salvas
(df_premio_individual_dono.count(), len(df_premio_individual_dono.columns))

# COMMAND ----------

# Checagem do valor total a ser pago após a separação de missao interna do dono
print(
    f'Valor total a pagar da rodada passada: {df_premio_individual_dono.filter(F.col("data_inicio") == data_inicio_missao_atual).agg(F.sum("premio_valor")).collect()[0][0]}'
)

print(
    f'Valor total a pagar da rodada retrasada: {(df_premio_individual_dono.filter(F.col("data_inicio") == data_inicio_missao_passada).agg(F.sum("premio_valor")).collect()[0][0])}'
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salva a apuração no sheets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apuração passada

# COMMAND ----------

data_inicio_missao_atual

# COMMAND ----------

salvar_resultados_google_sheets(
    df_premio_individual_maggu.filter(F.col('data_inicio') == data_inicio_missao_atual),
    SHEET_ID,
    SHEET_NAME_MISSAO_MAGGU,
    credentials,
)

# COMMAND ----------

salvar_resultados_google_sheets(
    df_premio_individual_dono.filter(F.col('data_inicio') == data_inicio_missao_atual),
    SHEET_ID,
    SHEET_NAME_MISSAO_INTERNA,
    credentials,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apuração retrasada

# COMMAND ----------

data_inicio_missao_passada

# COMMAND ----------

display(
    df_premio_individual_maggu.filter(
        F.col('data_inicio') == data_inicio_missao_passada
    ).select(
        'data_inicio',
        'semana_do_ano',
        'cnpj',
        'rede',
        'nome_loja',
        'username',
        'tipo_premio',
        'cpf',
        'nome_completo',
        'email',
        'telefone',
        F.col("premio_valor").alias('nova_apuracao'),
    )
)

# COMMAND ----------

# apuração da missão interna do dono da rodada passada
display(
    df_premio_individual_dono.filter(
        F.col('data_inicio') == data_inicio_missao_passada
    ).select(
        'data_inicio',
        'semana_do_ano',
        'cnpj',
        'rede',
        'nome_loja',
        'username',
        'tipo_premio',
        'cpf',
        'nome_completo',
        'email',
        'telefone',
        F.col("premio_valor").alias('nova_apuracao'),
    )
)
