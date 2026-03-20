# Databricks notebook source
# MAGIC %md
# MAGIC # Calcular Bônus para Gerentes
# MAGIC
# MAGIC Este notebook calcula bônus para gerentes baseado no desempenho dos atendentes:
# MAGIC 1. Conta quantos atendentes atingiram cada nível por loja
# MAGIC 2. Verifica se atingiu o número mínimo de atendentes necessário
# MAGIC 3. Encontra gerentes da loja e divide o bônus entre eles
# MAGIC 4. Salva resultados no Google Sheets

# COMMAND ----------

# MAGIC %pip install gspread google-auth

# COMMAND ----------

import json
from functools import reduce
from typing import Optional

import gspread
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from google.oauth2.service_account import Credentials
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import (
    IntegerType,
    StringType,
)

from maggulake.ativacao.whatsapp.regua_atendente.mappings import (
    mapear_nivel_para_status,
    mapear_status_para_nivel,
)
from maggulake.environment import DatabricksEnvironmentBuilder

# COMMAND ----------

dbutils.widgets.text(
    "google_sheet_id",
    "1KnQOrT0Gez5UaU5wGl8xfPDCY3msnZpLP_HV9Xvpgm4",
    "ID do Sheets",
)
dbutils.widgets.text("regras_worksheet_name", "Regras Premiação", "Aba das Regras")
dbutils.widgets.text(
    "resultado_missoes_maggu_ativacao_worksheet_name",
    "Missões Maggu - Ativacao",
    "Aba Missões Maggu Ativacao",
)
dbutils.widgets.text(
    "resultado_missoes_internas_ativacao_worksheet_name",
    "Missões Internas - Ativacao",
    "Aba Missões Internas Ativacao",
)
dbutils.widgets.text(
    "resultado_missoes_maggu_cs_worksheet_name",
    "Missões Maggu - CS",
    "Aba Missões Maggu CS",
)
dbutils.widgets.text(
    "resultado_missoes_internas_cs_worksheet_name",
    "Missões Internas - CS",
    "Aba Missões Internas CS",
)
dbutils.widgets.text(
    "regra_rede_ativa_cs_worksheet_name",
    "Regras Campanha Gerente",
    "Aba Regras Campanha Gerente CS",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações

# COMMAND ----------

GOOGLE_SHEET_ID = dbutils.widgets.get("google_sheet_id")
REGRAS_WORKSHEET_NAME = dbutils.widgets.get("regras_worksheet_name")
REGRA_REDE_ATIVA_CS = dbutils.widgets.get("regra_rede_ativa_cs_worksheet_name")
RESULTADO_MISSOES_MAGGU_ATIVACAO_WORKSHEET_NAME = dbutils.widgets.get(
    "resultado_missoes_maggu_ativacao_worksheet_name"
)
RESULTADO_MISSOES_INTERNAS_ATIVACAO_WORKSHEET_NAME = dbutils.widgets.get(
    "resultado_missoes_internas_ativacao_worksheet_name"
)
RESULTADO_MISSOES_MAGGU_CS_WORKSHEET_NAME = dbutils.widgets.get(
    "resultado_missoes_maggu_cs_worksheet_name"
)
RESULTADO_MISSOES_INTERNAS_CS_WORKSHEET_NAME = dbutils.widgets.get(
    "resultado_missoes_internas_cs_worksheet_name"
)

if not GOOGLE_SHEET_ID:
    raise ValueError(
        "Por favor, forneça o ID da planilha Google Sheets no widget 'google_sheet_id'"
    )


# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "calcular_bonus_gerentes",
    dbutils,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Spark e Conexões

# COMMAND ----------

spark = env.spark

# COMMAND ----------

# Data de referência centralizada para filtros
current_date = F.current_date()

# pega a data inicio da rodada que será apurada
data_inicio_missao = (
    spark.range(1)
    .select(
        F.date_format(
            F.date_sub(F.next_day(current_date, "Mon"), 14), "yyyy-MM-dd"
        ).alias("data_inicio_missao")
    )
    .first()["data_inicio_missao"]
)

print(data_inicio_missao)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de atendentes do airtable
# MAGIC - está mirando no airtable pois ainda tem muitas inconsistências no processo de indentificação de cargo dos usuários, especialmente com a tools que não possui essa segmentação

# COMMAND ----------

# o ideal é pegar os dados do postgres e não do BQ para pegar dados mais atualizados possível
postgres = env.postgres_replica_adapter

# COMMAND ----------

aa = postgres.read_table(spark, "atendentes_atendente").alias("aa")
ai = postgres.read_table(spark, "atendentes_informacoespessoais").alias("ai")
al = (
    postgres.read_table(spark, "atendentes_atendenteloja")
    .select('atendente_id', 'loja_id', 'cargo')
    .alias("al")
)

df_users = (
    aa.join(ai, aa.id == ai.atendente_id, how="inner")
    .join(al, aa.id == al.atendente_id, how="inner")
    .drop(aa.id)
    .drop(ai.id)
    .drop(al.atendente_id)
    .withColumnRenamed("atendente_id", "id")
    .withColumn(
        "username", F.lower(F.regexp_replace(F.col("aa.username"), r"^\s+|\s+$", ""))
    )
    .select(
        "id",
        "username",
        F.col("aa.first_name").alias("nome_completo"),
        "aa.email",
        "ai.cpf",
        F.col("ai.telefone").alias('whatsapp'),
        F.col('aa.is_active').alias('status'),
        'aa.conta_id',
        'al.loja_id',
        'al.cargo',
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções Auxiliares

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


def ler_dados_planilha(
    sheet_id: str, worksheet_name: str, credentials: Credentials
) -> DataFrame:
    try:
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)

        data = worksheet.get_all_records()

        if not data:
            raise ValueError(f"Nenhum dado encontrado na aba '{worksheet_name}'")

        df_pandas = pd.DataFrame(data)
        df_spark = spark.createDataFrame(df_pandas)

        return df_spark

    except Exception as e:
        raise RuntimeError(f"Erro ao ler dados da planilha {worksheet_name}: {str(e)}")


def append_novas_redes_google_sheets(
    novas_redes: list[str],
    sheet_id: str,
    worksheet_name: str,
    credentials: Credentials,
) -> None:
    if not novas_redes:
        return

    try:
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)

        headers = worksheet.row_values(1)
        num_cols = len(headers)

        try:
            rede_col_idx = next(
                i for i, h in enumerate(headers) if h.strip().lower() == "rede"
            )
        except StopIteration:
            raise ValueError(
                f"Coluna 'rede' não encontrada no cabeçalho da aba '{worksheet_name}'. "
                f"Colunas encontradas: {headers}"
            )

        rows_to_append = []
        for rede in novas_redes:
            row = [""] * num_cols
            row[rede_col_idx] = rede
            rows_to_append.append(row)

        worksheet.append_rows(rows_to_append)
        print(f"✅ {len(novas_redes)} novas redes adicionadas em '{worksheet_name}'")

    except Exception as e:
        raise RuntimeError(f"Erro ao fazer append de novas redes: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##  Atualiza planilhas com novas redes com `status = ATIVA`
# MAGIC - Adiciona novas redes com `status = ATIVA` para o time de cs fazer o opt-in de novas redes para a apuração

# COMMAND ----------

credentials = get_google_credentials()

df_redes_iniciais = ler_dados_planilha(
    GOOGLE_SHEET_ID, REGRA_REDE_ATIVA_CS, credentials
)

redes_planilha = [
    row["redes"]
    for row in df_redes_iniciais.select('redes').distinct().collect()
    if row["redes"] is not None
]
redes_ativas_db = [
    row["databricks_tenant_name"]
    for row in postgres.get_contas(spark)
    .filter(F.col("status") == "ATIVA")
    .select("databricks_tenant_name")
    .distinct()
    .collect()
    if row["databricks_tenant_name"] is not None
]
novas_redes_para_add = [r for r in redes_ativas_db if r not in redes_planilha and r]

if novas_redes_para_add:
    append_novas_redes_google_sheets(
        novas_redes_para_add, GOOGLE_SHEET_ID, REGRA_REDE_ATIVA_CS, credentials
    )
    df_redes = ler_dados_planilha(
        GOOGLE_SHEET_ID, REGRA_REDE_ATIVA_CS, credentials
    ).cache()
else:
    df_redes = df_redes_iniciais.cache()


# COMMAND ----------

redes_ativas = [
    row["redes"]
    for row in df_redes.filter(F.col('status') == True)
    .select('redes')
    .distinct()
    .collect()
    if row["redes"] is not None
]

redes_hibridas = [
    row["redes"]
    for row in (
        df_redes.filter(
            (F.col('status') == True) & (F.col('usar_regra_em_ativacao') == True)
        )
        .select('redes')
        .distinct()
        .collect()
    )
    if row["redes"] is not None
]

# COMMAND ----------


def _get_lojas(em_ativacao: bool = False) -> DataFrame:
    status_filtro = "EM_ATIVACAO" if em_ativacao else "ATIVA"
    df = (
        spark.read.table('bigquery.postgres_public.contas_loja')
        .filter(F.col("status") == status_filtro)
        .withColumn("loja_id_ref", F.col("id"))
        .select(
            "id",
            "loja_id_ref",
            "name",
            "status",
            F.col("databricks_tenant_name").alias('conta'),
            "cnpj",
        )
    )
    if not em_ativacao:
        df = df.filter(F.col('databricks_tenant_name').isin(redes_ativas))
    return df


def _get_missoes(interna: bool = False) -> DataFrame:
    df = (
        spark.read.table('bigquery.postgres_public.gameficacao_missao')
        .filter(F.to_date(F.col('data_inicio')) <= current_date)  # missoes ativas
        .filter(F.to_date(F.col('data_termino')) >= current_date)  # missoes ativas
        .withColumnRenamed('id', 'missao_id')
    )
    if interna:
        return df.filter(F.lower(F.col('nome')).contains('interna'))
    return df.filter(
        ~F.lower(F.col('nome')).contains('interna')
    )  # desconsiderar missoes internas


def _get_pontos_atendentes_com_loja(
    data_inicio: str,
    df_users: DataFrame,
    missao_interna: bool = False,
    apuracao_ativacao: bool = False,
) -> DataFrame:
    df_usuarios = df_users.select("id", "username")

    missoes = _get_missoes(interna=missao_interna)

    missoes_ids = missoes.select("missao_id").collect()
    missoes_ids_list = [f"{str(row.missao_id)}" for row in missoes_ids]

    df_pontos = (
        spark.read.table('bigquery.postgres_public.gameficacao_saldodepontos')
        .select(
            "atendente_id", "rodada_id", "missao_id", "nivel_atual", "atualizado_em"
        )
        .filter(F.col('missao_id').isin(missoes_ids_list))
    )
    lojas = _get_lojas(em_ativacao=apuracao_ativacao)
    lojas_ids = lojas.select("loja_id_ref").collect()
    lojas_ids_list = [f"'{str(row.loja_id_ref)}'" for row in lojas_ids]
    lojas_filter = f"AND v.loja_id IN ({', '.join(lojas_ids_list)})"

    vendas_query = f"""
    SELECT
        vi.atendente_id,
        ANY_VALUE(vi.rodada_id) as rodada_id,
        ANY_VALUE(vi.venda_id) as venda_id,
        ANY_VALUE(v.loja_id) as loja_id
    FROM
        bigquery.postgres_public.vendas_item AS vi
    INNER JOIN
        bigquery.postgres_public.vendas_venda AS v ON v.id = vi.venda_id
    INNER JOIN
        bigquery.postgres_public.gameficacao_rodada AS r ON vi.rodada_id = r.id
    WHERE
        r.data_inicio = '{data_inicio}'
        {lojas_filter}
        AND v.atendente_id IS NOT NULL
        AND vi.rodada_id IS NOT NULL
        AND v.loja_id IS NOT NULL
    GROUP BY
        vi.atendente_id
    """

    df_vendas = spark.sql(vendas_query)

    df_pontos_vendas = df_pontos.join(
        df_vendas,
        (df_pontos.atendente_id == df_vendas.atendente_id)
        & (df_pontos.rodada_id == df_vendas.rodada_id),
        "inner",
    ).select(
        df_vendas["*"],
        df_pontos["missao_id"],
        df_pontos["nivel_atual"],
        df_pontos["atualizado_em"],
    )

    return (
        df_usuarios.alias('u')
        .join(
            df_pontos_vendas.alias('vi'),
            F.col('u.id') == F.col('vi.atendente_id'),
            "inner",
        )
        .drop('id')
    )


def _get_latest_nivel(df: DataFrame) -> DataFrame:
    janela_nivel = Window.partitionBy("username").orderBy(F.desc("atualizado_em"))
    return (
        df.withColumn("row", F.row_number().over(janela_nivel))
        .filter(
            "row = 1"
        )  # contabiliza apenas 1 vez o nível por rodada, independente da quantidade de missões
        .drop("row")
        .dropDuplicates(["username"])
    )


def obter_dados_atendentes(
    missao_interna: bool = False, apuracao_ativacao: bool = False
) -> DataFrame:
    try:
        data_inicio = data_inicio_missao

        df_nivel_atual_raw = _get_pontos_atendentes_com_loja(
            data_inicio,
            df_users,
            missao_interna=missao_interna,
            apuracao_ativacao=apuracao_ativacao,
        ).cache()

        return _get_latest_nivel(df_nivel_atual_raw)

    except Exception as e:
        raise RuntimeError(f"Erro ao obter dados dos atendentes: {str(e)}")


# COMMAND ----------

# TODO: poderia migrar para o metodo `env.postgres_replica_adapter.get_gerentes_lojas()`


def obter_dados_gerentes(
    df_users: DataFrame,
    apuracao_ativacao: bool = False,
) -> DataFrame:
    try:
        lojas = _get_lojas(em_ativacao=apuracao_ativacao)

        return (
            df_users.alias('u')
            .filter(F.col("cargo") == 'GERENTE')
            .filter(F.col("status") == True)
            .join(
                lojas.alias('l').select('loja_id_ref', 'id'),
                F.col('u.loja_id') == F.col('l.id'),
                "inner",
            )
            .select('username', 'cargo', F.col('l.loja_id_ref').alias('loja_id'))
            .distinct()
        )

    except Exception as e:
        raise RuntimeError(f"Erro ao obter dados dos gerentes: {str(e)}")


def calcular_bonus_gerentes(
    regras_premiacao: DataFrame,
    dados_atendentes: DataFrame,
    dados_gerentes: DataFrame,
    df_users: DataFrame,
    apuracao_ativacao: bool = False,
    redes_hibridas: Optional[list[str]] = None,
) -> DataFrame:
    if redes_hibridas is None:
        redes_hibridas = []

    window_status = Window.partitionBy("loja_id", "nivel_status")
    window_loja = Window.partitionBy("loja_id")
    map_nivel_udf = F.udf(mapear_nivel_para_status, StringType())
    mapear_nivel_num_udf = F.udf(mapear_status_para_nivel, IntegerType())
    data_inicio = data_inicio_missao

    df_lojas = _get_lojas(em_ativacao=apuracao_ativacao).cache()

    dados_com_loja = (
        dados_atendentes.join(
            df_lojas, dados_atendentes.loja_id == df_lojas.id, "inner"
        )
        .select(
            dados_atendentes["*"],
            df_lojas.name.alias("nome_loja"),
            df_lojas.status.alias("status_loja"),
            df_lojas.conta.alias("conta"),
        )
        .withColumn(
            "status_para_regra",
            F.when(
                (F.col("conta").isin(redes_hibridas))
                & (F.col("status_loja") == "ATIVA"),
                F.lit("EM_ATIVACAO"),
            ).otherwise(F.col("status_loja")),
        )
    )

    dados_com_status = dados_com_loja.withColumn(
        "nivel_status",
        F.when(F.col("nivel_atual").isNull(), F.lit("nenhum")).otherwise(
            map_nivel_udf(F.col("nivel_atual"))
        ),
    ).withColumn(
        'nivel_status_num',
        F.when(F.col("nivel_status").isNull(), F.lit(0)).otherwise(
            mapear_nivel_num_udf(F.col("nivel_status"))
        ),
    )

    contagem_niveis = dados_com_status.filter(
        F.col("nivel_status").isNotNull()
        & (F.col("nivel_status") != "nenhum")
        & (
            (
                (F.col("status_para_regra") == "EM_ATIVACAO")
                & (F.col("nivel_status_num") >= 3)
            )
            | (
                (F.col("status_para_regra") == "ATIVA")
                & (F.col("nivel_status_num") == 5)
            )
        )
    )
    # contagem de atendentes por nivel atingido
    contagem_niveis = (
        contagem_niveis.withColumn(
            "qtde_atendentes_por_nivel",
            F.map_from_entries(
                F.collect_set(
                    F.struct(
                        F.col("nivel_status"),
                        F.count("nivel_status").over(window_status).cast("int"),
                    )
                ).over(window_loja)
            ),
        )
        .groupBy(
            "loja_id",
            "status_loja",
            "status_para_regra",
            "nome_loja",
            "conta",
            "qtde_atendentes_por_nivel",
        )
        .agg(F.count("*").alias("quantidade_atendentes_atual"))
    )

    regras_atingidas = (
        regras_premiacao.alias("regras")
        .join(
            contagem_niveis.alias("contagem"),
            (F.col("regras.status_loja") == F.col("contagem.status_para_regra")),
            "inner",
        )
        .filter(
            F.col("contagem.quantidade_atendentes_atual")
            >= F.col("regras.quantidade_atendentes_no_nivel")
        )
        .select(
            F.col("contagem.loja_id").alias("loja_id"),
            F.col("contagem.status_loja").alias("status_loja"),
            F.col("contagem.nome_loja").alias("nome_loja"),
            F.col("contagem.conta").alias("conta"),
            F.col("contagem.quantidade_atendentes_atual").alias(
                "quantidade_atendentes_atual"
            ),
            F.col("contagem.qtde_atendentes_por_nivel").alias(
                'qtde_atendentes_por_nivel'
            ),
            F.col("regras.quantidade_atendentes_no_nivel").alias(
                "quantidade_atendentes_no_nivel"
            ),
            F.col("regras.premiacao").alias("premiacao"),
        )
    )

    maior_bonus_por_loja = regras_atingidas.groupBy(
        "loja_id", "status_loja", "nome_loja"
    ).agg(F.max("premiacao").alias("maior_premiacao"))

    regra_maior_bonus = (
        regras_atingidas.alias("regras_at")
        .join(
            maior_bonus_por_loja.alias("maior"),
            (F.col("regras_at.loja_id") == F.col("maior.loja_id"))
            & (F.col("regras_at.premiacao") == F.col("maior.maior_premiacao")),
            "inner",
        )
        .select(
            F.col("regras_at.loja_id").alias("loja_id"),
            F.col("regras_at.status_loja").alias("status_loja"),
            F.col("regras_at.nome_loja").alias("nome_loja"),
            F.col('regras_at.conta').alias('conta'),
            F.col("regras_at.quantidade_atendentes_atual").alias(
                "quantidade_atendentes_atual"
            ),
            F.col("regras_at.quantidade_atendentes_no_nivel").alias(
                "quantidade_atendentes_no_nivel"
            ),
            F.col("regras_at.premiacao").alias("premiacao"),
            F.col('regras_at.qtde_atendentes_por_nivel')
            .cast("string")
            .alias("qtde_atendentes_por_nivel"),
        )
        .distinct()
    )

    gerentes_por_loja = dados_gerentes.groupBy("loja_id").agg(
        F.count("username").alias("num_gerentes")
    )

    bonus_com_divisao = (
        regra_maior_bonus.alias("bonus")
        .join(
            gerentes_por_loja.alias("ger_count"),
            F.col("bonus.loja_id") == F.col("ger_count.loja_id"),
            "left",
        )
        .select(
            F.col("bonus.*"),
            F.col("ger_count.num_gerentes").alias("num_gerentes"),
            F.round(
                F.col("bonus.premiacao") / F.col("ger_count.num_gerentes"), 2
            ).alias("bonus_dividido"),
        )
    )

    gerentes_com_loja = dados_gerentes.join(
        df_lojas, dados_gerentes.loja_id == df_lojas.id, "inner"
    ).select(
        dados_gerentes["*"],
        df_lojas.name.alias("nome_loja"),
        df_lojas.status.alias("status_loja"),
        df_lojas.conta,
    )

    resultado_final = (
        bonus_com_divisao.alias("bonus_div")
        .join(
            gerentes_com_loja.alias("ger"),
            F.col("bonus_div.loja_id") == F.col("ger.loja_id"),
            "left",
        )
        .join(
            df_users.alias("air"),
            (F.col("ger.username") == F.col("air.username"))
            & (F.col('ger.loja_id') == F.col('air.loja_id')),
            "left",
        )
        .withColumn('semana', F.lit(data_inicio))
        .select(
            F.col('semana'),
            F.col("ger.username").alias("username_gerente"),
            F.col("air.nome_completo").alias('nome_completo'),
            F.col("air.cpf").alias("cpf"),
            F.col("air.whatsapp").alias('whatsapp'),
            F.col("ger.cargo").alias("cargo"),
            F.col("air.email").alias("email"),
            F.coalesce(F.col("bonus_div.nome_loja"), F.col('ger.nome_loja')).alias(
                "nome_loja"
            ),
            F.coalesce(F.col("bonus_div.status_loja"), F.col('ger.status_loja')).alias(
                "status_loja"
            ),
            F.coalesce(F.col("bonus_div.conta"), F.col('ger.conta')).alias("conta"),
            F.coalesce(
                F.col("bonus_div.bonus_dividido"), F.col("bonus_div.premiacao")
            ).alias("bonus_dividido_por_gerente"),
            F.col("bonus_div.quantidade_atendentes_atual").alias("qtd_atendentes"),
            F.col("bonus_div.qtde_atendentes_por_nivel").alias(
                "qtde_atendentes_por_nivel"
            ),
        )
    )

    return resultado_final


def salvar_resultados_google_sheets(
    df_resultado: DataFrame,
    sheet_id: str,
    worksheet_name: str,
    credentials: Credentials,
) -> None:
    try:
        df_pandas = df_resultado.toPandas()

        if df_pandas.empty:
            print("Nenhum resultado para salvar")
            return

        # Substitui valores nulos e infinitos por string vazia
        df_pandas = df_pandas.replace([np.inf, -np.inf, np.nan], "")

        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(sheet_id)

        try:
            worksheet = sheet.worksheet(worksheet_name)
            # Lê os dados existentes para evitar apagar
            dados_existentes = worksheet.get_all_records()
            df_existente = pd.DataFrame(dados_existentes)

            # Concatena os novos dados com os existentes
            df_completo = pd.concat([df_existente, df_pandas], ignore_index=True)
            df_completo = df_completo.fillna("")

            # Limpa a planilha para reaproveitar o espaço
            worksheet.clear()
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(
                title=worksheet_name, rows=1000, cols=max(13, len(df_pandas.columns))
            )
            df_completo = df_pandas

        headers = list(df_completo.columns)
        values = [headers] + df_completo.values.tolist()

        worksheet.update(values, 'A1')

    except Exception as e:
        raise RuntimeError(f"Erro ao salvar resultados: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução Principal

# COMMAND ----------


def salvar_historico_apuracao(apuracao_ativacao: bool) -> None:
    resultados = []
    for eh_missao_interna in [True, False]:
        try:
            credentials = get_google_credentials()
            regras_premiacao = ler_dados_planilha(
                GOOGLE_SHEET_ID, REGRAS_WORKSHEET_NAME, credentials
            )
            dados_atendentes = obter_dados_atendentes(
                missao_interna=eh_missao_interna, apuracao_ativacao=apuracao_ativacao
            )
            dados_gerentes = obter_dados_gerentes(
                df_users, apuracao_ativacao=apuracao_ativacao
            )

            resultado_bonus = calcular_bonus_gerentes(
                regras_premiacao,
                dados_atendentes,
                dados_gerentes,
                df_users,
                apuracao_ativacao=apuracao_ativacao,
                redes_hibridas=redes_hibridas,
            )

            tipo_str = "interna" if eh_missao_interna else "maggu"
            df_result = resultado_bonus.withColumn("tipo_missao", F.lit(tipo_str))
            resultados.append(df_result)

        except Exception as e:
            print(
                f"Erro no processamento missao_interna={eh_missao_interna}, apuracao_ativacao={apuracao_ativacao}: {str(e)}"
            )
            raise

    df_union = reduce(DataFrame.unionByName, resultados)

    nome_tabela = (
        "hive_metastore.pbi.apuracao_gerentes_ativacao"
        if apuracao_ativacao
        else "hive_metastore.pbi.apuracao_gerentes_cs"
    )

    df_union.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(nome_tabela)

    print(f"✅ Resultados salvos na tabela delta {nome_tabela}")


# COMMAND ----------


def executar_processo(
    missao_interna: bool = False, apuracao_ativacao: bool = False
) -> None:
    try:
        if missao_interna and apuracao_ativacao:
            aba_planilha = RESULTADO_MISSOES_INTERNAS_ATIVACAO_WORKSHEET_NAME
        elif missao_interna and not apuracao_ativacao:
            aba_planilha = RESULTADO_MISSOES_INTERNAS_CS_WORKSHEET_NAME
        elif not missao_interna and apuracao_ativacao:
            aba_planilha = RESULTADO_MISSOES_MAGGU_ATIVACAO_WORKSHEET_NAME
        elif not missao_interna and not apuracao_ativacao:
            aba_planilha = RESULTADO_MISSOES_MAGGU_CS_WORKSHEET_NAME
        else:
            raise ValueError("Combinação de parâmetros inválida")

        credentials = get_google_credentials()

        regras_premiacao = ler_dados_planilha(
            GOOGLE_SHEET_ID, REGRAS_WORKSHEET_NAME, credentials
        )
        dados_atendentes = obter_dados_atendentes(
            missao_interna=missao_interna, apuracao_ativacao=apuracao_ativacao
        )

        dados_gerentes = obter_dados_gerentes(
            df_users, apuracao_ativacao=apuracao_ativacao
        )

        resultado_bonus = calcular_bonus_gerentes(
            regras_premiacao,
            dados_atendentes,
            dados_gerentes,
            df_users,
            apuracao_ativacao=apuracao_ativacao,
            redes_hibridas=redes_hibridas,
        )

        print("Resultados premiação:")
        resultado_bonus.display()

        salvar_resultados_google_sheets(
            resultado_bonus,
            GOOGLE_SHEET_ID,
            aba_planilha,
            credentials,
        )
        print("\n✅ Processo concluído com sucesso!")

    except Exception as e:
        print(f"❌ Erro durante a execução: {str(e)}")
        raise


# COMMAND ----------

# MAGIC %md
# MAGIC Apuração Time Ativação

# COMMAND ----------

# Apuração missões internas
executar_processo(missao_interna=True, apuracao_ativacao=True)

# COMMAND ----------

# Apuração missões maggu
executar_processo(missao_interna=False, apuracao_ativacao=True)

# COMMAND ----------

# salvar o historico de apuracao
salvar_historico_apuracao(apuracao_ativacao=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Apuração Time CS

# COMMAND ----------

# Apuração missões internas
executar_processo(missao_interna=True, apuracao_ativacao=False)

# COMMAND ----------

# Apuração missões internas
executar_processo(missao_interna=False, apuracao_ativacao=False)

# COMMAND ----------

# salvar historico de apuração
salvar_historico_apuracao(apuracao_ativacao=False)
