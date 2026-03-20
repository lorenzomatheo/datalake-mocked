# Databricks notebook source
# MAGIC %md
# MAGIC # Acompanhamento Hyperflow — Cadastro de Atendentes
# MAGIC
# MAGIC Este notebook analisa o cadastro de atendentes via Hyperflow e persiste views no Databricks
# MAGIC para alimentar dashboards.
# MAGIC
# MAGIC **Tabelas geradas** (schema `{catalog}.analytics`):
# MAGIC - `hyperflow_cadastros_base` — base desnormalizada: atendente x loja x flag passou_hyperflow
# MAGIC - `hyperflow_nsm` — NSM por execução: cadastros / ativos 30d (snapshot com timestamp)
# MAGIC - `hyperflow_cadastros_por_loja` — totais por loja (elegiveis, com hyperflow, ativos 30d)

# COMMAND ----------

# MAGIC %pip install oauth2client gspread

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.environment.tables import Table
from maggulake.io.google_sheets import get_client, get_spreadsheet
from maggulake.schemas import (
    schema_hyperflow_cadastros_base,
    schema_hyperflow_cadastros_por_loja,
    schema_hyperflow_cadastros_sheets,
    schema_hyperflow_nsm,
)

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build("acompanhamento_hyperflow", dbutils)
CATALOG = env.settings.catalog
postgres = env.postgres_replica_adapter

print(f"Catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constantes

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# TODO: Como essa tabela tem cpf, telefone, chave_pix, etc vale deixar dentro de um secrtes
# Gui: Mas o proprio google sheets ja controla permissoes... talvez nao valha a pena.
GOOGLE_SHEETS_LINK = "https://docs.google.com/spreadsheets/d/1qgH4e4RcNqci0kWcogUkq_n8-DUW5k__i2GoHvv7Mp0/edit?gid=1430248852#gid=1430248852"
NOME_ABA_SHEETS = "pix_atendentes_hyperflow"

CODIGOS_LOJA_EXCLUIDOS = [
    "LKXYH4",  # Loja Maggu em staging
    "XRLJ48",  # Loja teste
]
DATA_INICIO_HYPERFLOW = pd.Timestamp("2025-09-01")

# Filtramos para lojas ativas ou em ativacao pois sao as que tem mais chance de terem atendentes passando pelo Hyperflow.
# Lojas CHURN não são importantes para acompanharmos
# TODO: rever a necessidade desse filtro
FILTRO_STATUS_LOJA = ["ATIVA", "EM_ATIVACAO"]

# Consideramos apenas "cadastrado" pois os outros status (ex: "pre-cadastro") indicam que o atendente ainda nao concluiu o flow de cadastro no hyperflow
FILTRO_STATUS_ATIVACAO = ["cadastrado"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criacao das tabelas (se nao existirem)

# COMMAND ----------

tables_to_create = [
    (Table.hyperflow_cadastros_sheets, schema_hyperflow_cadastros_sheets),
    (Table.hyperflow_cadastros_base, schema_hyperflow_cadastros_base),
    (Table.hyperflow_nsm, schema_hyperflow_nsm),
    (Table.hyperflow_cadastros_por_loja, schema_hyperflow_cadastros_por_loja),
]
for table, schema in tables_to_create:
    env.create_table_if_not_exists(table, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Credenciais Google Sheets

# COMMAND ----------

gsheet_scope = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(
        dbutils.secrets.get(scope="databricks", key="GOOGLE_SHEETS_CREDENTIALS")
    ),
    gsheet_scope,
)

client = get_client(credentials=CREDENTIALS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura Google Sheets

# COMMAND ----------

sheets_file = get_spreadsheet(
    sheet_url=GOOGLE_SHEETS_LINK, credentials=CREDENTIALS, client=client
)
records = sheets_file.worksheet(NOME_ABA_SHEETS).get_all_records()
df_sheets = pd.DataFrame(records)


# COMMAND ----------

# Normalizacao de datas eh importante pois no sheets salvamos de um jeito bem especifico.
# Usamos errors="raise" pois a data é sempre gerada pelo Hyperflow (sem input manual),
# então qualquer falha de parse indica um bug que precisa ser corrigido.
df_sheets["confirmou_chave_em"] = pd.to_datetime(
    df_sheets["confirmou_chave_em"],
    format="%Y-%m-%d_%H:%M:%S.%f",
    errors="raise",
)

# COMMAND ----------

# Nao pode olhar pro que foi feito em modo teste
df_sheets = df_sheets[df_sheets["confirmou_chave_em"] >= DATA_INICIO_HYPERFLOW]
df_sheets = df_sheets[~df_sheets["codigo_loja"].isin(CODIGOS_LOJA_EXCLUIDOS)]


# COMMAND ----------

# Tem que tirar os registros com `_error`, pois são tentativas má sucedidas de cadastro
antes = len(df_sheets)
df_sheets = df_sheets[~df_sheets["chave"].str.contains("_error", na=False)]
depois = len(df_sheets)

print(f"Removidos {antes - depois} registros pois tratam de erros durante o flow.")
print(
    f"Isso representa {depois / antes * 100:.2f}% dos registros."
    if antes > 0
    else "Nenhum registro encontrado."
)

# COMMAND ----------

# Drop informacoes que nao sao tao relevantes nesse notebook
df_sheets = df_sheets.drop(["chave", "tipo_chave"], axis=1)


# COMMAND ----------

if DEBUG:
    print(f"Registros no Google Sheets do Hyperflow: {len(df_sheets)}")
    print(
        f"Atendentes distintos (nome_completo): {df_sheets['nome_completo'].nunique()}"
    )
    print(f"Lojas distintas: {df_sheets['codigo_loja'].nunique()}")
    display(df_sheets.head(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 ID unificado: username + codigo_externo

# COMMAND ----------

# Vectorized replacement of empty/blank strings to NaN for multiple columns
df_sheets[["username", "codigo_externo", "atendente_id"]] = df_sheets[
    ["username", "codigo_externo", "atendente_id"]
].replace(r"^\s*$", np.nan, regex=True)

# COMMAND ----------

# NOTE: id_usuario_unificado é usado apenas para o DEBUG abaixo. O matching real (seção 4) usa dicts separados por username e codigo_externo.
df_sheets["id_usuario_unificado"] = (
    df_sheets["codigo_externo"].fillna(df_sheets["username"]).astype(str)
)
df_sheets["codigo_loja"] = df_sheets["codigo_loja"].astype(str).str.strip()
df_sheets["username"] = df_sheets["username"].astype("string").str.strip()
df_sheets["codigo_externo"] = df_sheets["codigo_externo"].astype("string").str.strip()
df_sheets["atendente_id"] = pd.to_numeric(df_sheets["atendente_id"], errors="coerce")
df_sheets["data_cadastro_hyperflow"] = pd.to_datetime(df_sheets["confirmou_chave_em"])

chave_por_id = df_sheets["atendente_id"].apply(
    lambda valor: f"id:{int(valor)}" if pd.notna(valor) else pd.NA
)
chave_por_codigo_externo = df_sheets["codigo_externo"].where(
    df_sheets["codigo_externo"].notna(), pd.NA
)
chave_por_codigo_externo = chave_por_codigo_externo.apply(
    lambda valor: f"codigo_externo:{valor}" if pd.notna(valor) else pd.NA
)
chave_por_username = df_sheets["username"].where(df_sheets["username"].notna(), pd.NA)
chave_por_username = chave_por_username.apply(
    lambda valor: f"username:{valor}" if pd.notna(valor) else pd.NA
)

df_sheets["chave_deduplicacao"] = chave_por_id.fillna(chave_por_codigo_externo).fillna(
    chave_por_username
)
df_sheets["chave_deduplicacao"] = df_sheets["chave_deduplicacao"].fillna(
    "sem_identificador"
)

colunas_sheets_espelho = [
    "codigo_loja",
    "atendente_id",
    "username",
    "codigo_externo",
    "nome_completo",
    "confirmou_chave_em",
    "data_cadastro_hyperflow",
    "id_usuario_unificado",
    "chave_deduplicacao",
]
df_sheets_espelho = df_sheets.reindex(columns=colunas_sheets_espelho).copy()
df_sheets_espelho = (
    df_sheets_espelho.sort_values("data_cadastro_hyperflow")
    .drop_duplicates(subset=["codigo_loja", "chave_deduplicacao"], keep="last")
    .reset_index(drop=True)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura PostgreSQL
# MAGIC
# MAGIC Reutilizamos os helpers do maggulake onde possivel:
# MAGIC - `postgres.get_atendentes()` para a base global de atendentes
# MAGIC - `postgres.get_atendentes_com_lojas()` para o join atendente x loja x rede
# MAGIC - `postgres.read_table_pandas("atendentes_atendente")` para ultima_venda_realizada_em

# COMMAND ----------

df_atendentes_base = postgres.get_atendentes()


# COMMAND ----------


print(f"Total atendentes no banco: {len(df_atendentes_base)}")
print(f"\tCadastrados: {(df_atendentes_base['status_ativacao'] == 'cadastrado').sum()}")
print(
    f"\tCom data cadastrado_em preenchida: {df_atendentes_base['cadastrado_em'].notna().sum()}"
)
print(
    f"\tOpt-in comunicações whatsapp ativo: {df_atendentes_base['deve_receber_comunicacoes_no_whatsapp'].sum()}"
)

if DEBUG:
    display(df_atendentes_base.head(10))

# COMMAND ----------

# Atendentes ativos nos ultimos 30 dias = quem fez alguma venda nos últimos 30 dias
df_ativos_30d = postgres.execute_query("""
    SELECT id, username
    FROM atendentes_atendente
    WHERE ultima_venda_realizada_em IS NOT NULL
      AND ultima_venda_realizada_em >= NOW() - INTERVAL '30 days'
      AND ultima_venda_realizada_em <= NOW()
      AND is_active = true
""")

# COMMAND ----------

if DEBUG:
    print(f"Atendentes com venda nos ultimos 30 dias: {len(df_ativos_30d)}")
    display(df_ativos_30d.head(10))

# COMMAND ----------

df_atendentesloja_banco = postgres.get_atendentes_com_lojas(apenas_ativos=True)

# COMMAND ----------

if DEBUG:
    print(f"Atendentes-Loja presentes no banco: {len(df_atendentesloja_banco)}")
    df_atendentesloja_banco.head(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cruzamento Sheets x Banco

# COMMAND ----------

# Match por loja quando disponível no Sheets.
# Fallback global é aplicado apenas para linhas do Sheets sem codigo_loja.
df_sheets["codigo_loja"] = df_sheets["codigo_loja"].replace(
    {"": pd.NA, "nan": pd.NA, "None": pd.NA}
)

data_por_loja_id: dict = (
    df_sheets[df_sheets["codigo_loja"].notna() & df_sheets["atendente_id"].notna()]
    .groupby(["codigo_loja", "atendente_id"])["data_cadastro_hyperflow"]
    .max()
    .to_dict()
)
data_por_loja_username: dict = (
    df_sheets[df_sheets["codigo_loja"].notna() & df_sheets["username"].notna()]
    .groupby(["codigo_loja", "username"])["data_cadastro_hyperflow"]
    .max()
    .to_dict()
)
data_por_loja_codigo_externo: dict = (
    df_sheets[df_sheets["codigo_loja"].notna() & df_sheets["codigo_externo"].notna()]
    .groupby(["codigo_loja", "codigo_externo"])["data_cadastro_hyperflow"]
    .max()
    .to_dict()
)

data_global_username: dict = (
    df_sheets[df_sheets["codigo_loja"].isna() & df_sheets["username"].notna()]
    .groupby("username")["data_cadastro_hyperflow"]
    .max()
    .to_dict()
)
data_global_codigo_externo: dict = (
    df_sheets[df_sheets["codigo_loja"].isna() & df_sheets["codigo_externo"].notna()]
    .groupby("codigo_externo")["data_cadastro_hyperflow"]
    .max()
    .to_dict()
)

joined_df = df_atendentesloja_banco.copy()
joined_df["codigo_de_seis_digitos"] = (
    joined_df["codigo_de_seis_digitos"].astype(str).str.strip()
)
joined_df["username"] = joined_df["username"].astype("string").str.strip()
joined_df["codigo_externo_atendente"] = (
    joined_df["codigo_externo_atendente"].astype("string").str.strip()
)

key_loja_id = list(zip(joined_df["codigo_de_seis_digitos"], joined_df["id_atendente"]))
key_loja_user = list(zip(joined_df["codigo_de_seis_digitos"], joined_df["username"]))
key_loja_code = list(
    zip(joined_df["codigo_de_seis_digitos"], joined_df["codigo_externo_atendente"])
)

match_loja_id = pd.Series(
    [data_por_loja_id.get(k) for k in key_loja_id], index=joined_df.index
)
match_loja_user = pd.Series(
    [data_por_loja_username.get(k) for k in key_loja_user], index=joined_df.index
)
match_loja_code = pd.Series(
    [data_por_loja_codigo_externo.get(k) for k in key_loja_code], index=joined_df.index
)
match_global_user = joined_df["username"].map(data_global_username)
match_global_code = joined_df["codigo_externo_atendente"].map(
    data_global_codigo_externo
)

joined_df["data_cadastro_hyperflow"] = (
    match_loja_id.fillna(match_loja_user)
    .fillna(match_loja_code)
    .fillna(match_global_user)
    .fillna(match_global_code)
)
joined_df["passou_pelo_hyperflow"] = joined_df["data_cadastro_hyperflow"].notna()
joined_df["metodo_match_hyperflow"] = np.select(
    [
        match_loja_id.notna(),
        match_loja_user.notna(),
        match_loja_code.notna(),
        match_global_user.notna(),
        match_global_code.notna(),
    ],
    [
        "loja_id_atendente",
        "loja_username",
        "loja_codigo_externo",
        "global_username_sem_loja",
        "global_codigo_externo_sem_loja",
    ],
    default="sem_match",
)

# COMMAND ----------

n_usernames_sheets = df_sheets["username"].dropna().nunique()
n_codigos_sheets = df_sheets["codigo_externo"].dropna().nunique()
total_matches = int(joined_df["passou_pelo_hyperflow"].sum())

print(f"Usernames distintos no Sheets: {n_usernames_sheets}")
print(f"Codigos externos distintos no Sheets: {n_codigos_sheets}")

print(
    f"Atendentes-loja com match: {total_matches} de {len(joined_df)}"
    f" ({total_matches / max(len(joined_df), 1) * 100:.1f}%)"
)


# COMMAND ----------

# Inspecionar linhas que não tiveram match
unmatched_mask = ~joined_df["passou_pelo_hyperflow"].fillna(False)
df_unmatched = joined_df[unmatched_mask].copy()
print(f"Linhas sem match: {len(df_unmatched)}")

# Estatísticas rápidas para ajudar diagnóstico
missing_username = df_unmatched["username"].isna().sum()
missing_codigo_externo = df_unmatched["codigo_externo_atendente"].isna().sum()
print(
    f"Entre sem match - username ausente: {missing_username}, codigo_externo ausente: {missing_codigo_externo}"
)

if DEBUG:
    print("Distribuição de métodos de match Hyperflow:")
    print(joined_df["metodo_match_hyperflow"].value_counts(dropna=False))

if DEBUG:
    display(df_unmatched.head(50))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Base limpa: filtros de qualidade

# COMMAND ----------

# Filtra atendentes eligiíveis: lojas ativas/em ativação e status de ativação "cadastrado".
# Cada linha representa um par (atendente, loja) único. Um mesmo atendente pode aparecer em múltiplas lojas.
# As métricas globais (taxa geral, NSM) usam nunique("id_atendente") para evitar dupla contagem.

df_base_limpa = (
    joined_df[joined_df["status_loja"].isin(FILTRO_STATUS_LOJA)]
    .pipe(lambda df: df[df["status_ativacao"].isin(FILTRO_STATUS_ATIVACAO)])
    .copy()
)

df_base_limpa = df_base_limpa.merge(
    df_ativos_30d[["id"]].rename(columns={"id": "_id_ativo"}),
    left_on="id_atendente",
    right_on="_id_ativo",
    how="left",
)
df_base_limpa["ativo_ultimos_30d"] = df_base_limpa["_id_ativo"].notna()
df_base_limpa.drop(columns=["_id_ativo"], inplace=True)
df_base_limpa["passou_pelo_hyperflow"] = df_base_limpa["passou_pelo_hyperflow"].fillna(
    False
)

# Remove duplicidades de mesma relação atendente-loja para evitar inflar métricas.
chaves_atendente_loja = [
    "id_atendente",
    "id_loja",
    "codigo_de_seis_digitos",
    "nome_loja",
    "nome_rede",
    "erp",
    "status_loja",
    "cnpj_loja",
    "codigo_externo_atendente",
    "username",
    "status_ativacao",
    "deve_receber_comunicacoes_no_whatsapp",
    "nome_atendente",
    "telefone_atendente",
]
df_base_limpa = (
    df_base_limpa.sort_values("data_cadastro_hyperflow")
    .groupby(chaves_atendente_loja, dropna=False, as_index=False)
    .agg(
        passou_pelo_hyperflow=("passou_pelo_hyperflow", "max"),
        ativo_ultimos_30d=("ativo_ultimos_30d", "max"),
        data_cadastro_hyperflow=("data_cadastro_hyperflow", "max"),
    )
)

df_base_limpa["id_atendente_hyperflow"] = df_base_limpa["id_atendente"].where(
    df_base_limpa["passou_pelo_hyperflow"]
)
df_base_limpa["id_atendente_ativo_30d"] = df_base_limpa["id_atendente"].where(
    df_base_limpa["ativo_ultimos_30d"]
)

print(f"Base original: {len(joined_df)} registros")
print(f"Apos filtros: {len(df_base_limpa)} registros")
print(f"Atendentes únicos elegíveis: {df_base_limpa['id_atendente'].nunique()}")
print(
    f"Atendentes únicos ativos 30d: {df_base_limpa['id_atendente_ativo_30d'].nunique()}"
)
print(
    f"Atendentes únicos com Hyperflow: {df_base_limpa['id_atendente_hyperflow'].nunique()}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Analise exploratoria

# COMMAND ----------

# MAGIC %md
# MAGIC ### Taxa de adesao por loja

# COMMAND ----------

# Por loja: todas as métricas são contadas por atendente único dentro da loja.
taxa_adesao_por_loja = (
    df_base_limpa.groupby(
        ["codigo_de_seis_digitos", "nome_loja", "nome_rede", "erp", "status_loja"]
    )
    .agg(
        total_atendentes_elegiveis=("id_atendente", "nunique"),
        total_com_hyperflow=("id_atendente_hyperflow", "nunique"),
        total_ativos_30d=("id_atendente_ativo_30d", "nunique"),
    )
    .reset_index()
)
taxa_adesao_por_loja["taxa_adesao_percentual"] = (
    taxa_adesao_por_loja["total_com_hyperflow"]
    / taxa_adesao_por_loja["total_atendentes_elegiveis"]
    * 100
).round(2)
taxa_adesao_por_loja["total_com_hyperflow"] = taxa_adesao_por_loja[
    "total_com_hyperflow"
].astype(int)
taxa_adesao_por_loja["total_ativos_30d"] = taxa_adesao_por_loja[
    "total_ativos_30d"
].astype(int)
taxa_adesao_por_loja = taxa_adesao_por_loja.sort_values(
    "taxa_adesao_percentual", ascending=False
)

# Base global por atendente para métricas globais, ignorando granularidade atendente-loja.
df_base_global = df_base_limpa.groupby("id_atendente", as_index=False).agg(
    passou_pelo_hyperflow=("passou_pelo_hyperflow", "max"),
    ativo_ultimos_30d=("ativo_ultimos_30d", "max"),
)

total_atendentes_unicos = df_base_global["id_atendente"].nunique()
total_com_hyperflow_unicos = df_base_global[df_base_global["passou_pelo_hyperflow"]][
    "id_atendente"
].nunique()
taxa_geral = (
    total_com_hyperflow_unicos / total_atendentes_unicos * 100
    if total_atendentes_unicos > 0
    else 0.0
)
print(f"Total lojas: {len(taxa_adesao_por_loja)}")
print(f"Total atendentes eligiíveis (únicos): {total_atendentes_unicos}")
print(f"Total com HyperFlow (únicos): {total_com_hyperflow_unicos}")
print(f"Taxa de adesao geral: {taxa_geral:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persistencia: salvar tabelas Delta para dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### hyperflow_cadastros_sheets (espelho Google Sheets)

# COMMAND ----------

df_sheets_espelho_spark = (
    spark.createDataFrame(df_sheets_espelho)
    .withColumn("atendente_id", F.col("atendente_id").cast("int"))
    .withColumn("confirmou_chave_em", F.col("confirmou_chave_em").cast("timestamp"))
    .withColumn(
        "data_cadastro_hyperflow", F.col("data_cadastro_hyperflow").cast("timestamp")
    )
    .withColumn("atualizado_em", F.current_timestamp())
)

df_sheets_espelho_spark.write.format("delta").mode("overwrite").saveAsTable(
    Table.hyperflow_cadastros_sheets.value
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### hyperflow_cadastros_base

# COMMAND ----------

# TODO: valeria a pena unificar isso aqui num schema tbm...
df_base_spark = spark.createDataFrame(
    df_base_limpa[
        [
            "id_loja",
            "nome_loja",
            "codigo_de_seis_digitos",
            "status_loja",
            "cnpj_loja",
            "erp",
            "nome_rede",
            "codigo_externo_atendente",
            "id_atendente",
            "username",
            "status_ativacao",
            "deve_receber_comunicacoes_no_whatsapp",
            "nome_atendente",
            "telefone_atendente",
            "passou_pelo_hyperflow",
            "ativo_ultimos_30d",
            "data_cadastro_hyperflow",
        ]
    ]
).withColumn("atualizado_em", F.current_timestamp())

# COMMAND ----------

if DEBUG:
    env.table(Table.hyperflow_cadastros_base).limit(100).display()


# COMMAND ----------

df_base_spark.printSchema()

# COMMAND ----------

#  converter df_base_spark["id_atendente"] para integer
df_base_spark = df_base_spark.withColumn(
    "id_atendente", df_base_spark["id_atendente"].cast(IntegerType())
)

# COMMAND ----------

df_base_spark.write.format("delta").mode("overwrite").saveAsTable(
    Table.hyperflow_cadastros_base.value
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### hyperflow_nsm (append por execução)

# COMMAND ----------

# Todos os totais usam nunique("id_atendente") para não dupla-contar atendentes em múltiplas lojas
total_atendentes_base = df_base_limpa["id_atendente"].nunique()
total_ativos_30d_int = df_base_global[df_base_global["ativo_ultimos_30d"]][
    "id_atendente"
].nunique()
total_cadastros_hyperflow = df_base_global[df_base_global["passou_pelo_hyperflow"]][
    "id_atendente"
].nunique()
cadastros_em_ativos = df_base_global[
    df_base_global["ativo_ultimos_30d"] & df_base_global["passou_pelo_hyperflow"]
]["id_atendente"].nunique()
nsm_percentual = round(
    (cadastros_em_ativos / total_ativos_30d_int * 100)
    if total_ativos_30d_int > 0
    else 0.0,
    2,
)

# COMMAND ----------

df_nsm = spark.createDataFrame(
    [
        (
            datetime.now(timezone.utc),
            int(total_cadastros_hyperflow),
            int(total_ativos_30d_int),
            int(cadastros_em_ativos),
            nsm_percentual,
            int(total_atendentes_base),
            datetime.now(timezone.utc),
        )
    ],
    schema=schema_hyperflow_nsm,
)

df_nsm.write.format("delta").mode("append").saveAsTable(Table.hyperflow_nsm.value)

print(
    f"\tNSM execução atual: {nsm_percentual:.2f}% ({cadastros_em_ativos}/{total_ativos_30d_int})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### hyperflow_cadastros_por_loja

# COMMAND ----------

df_loja = (
    spark.createDataFrame(taxa_adesao_por_loja)
    .withColumn("atualizado_em", F.current_timestamp())
    .withColumn(
        "total_com_hyperflow", F.column("total_com_hyperflow").cast(IntegerType())
    )
    .withColumn("total_ativos_30d", F.column("total_ativos_30d").cast(IntegerType()))
)

df_loja.write.format("delta").mode("overwrite").saveAsTable(
    Table.hyperflow_cadastros_por_loja.value
)
