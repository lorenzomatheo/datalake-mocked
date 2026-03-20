# Databricks notebook source
# MAGIC %md # Update Postgres Manual Corrections from Google Sheets
# MAGIC
# MAGIC This notebook reads product manual corrections from Google Sheets
# MAGIC and updates the produtos_produtocorrecaomanual table in Postgres.

# COMMAND ----------

# MAGIC %pip install gspread google-auth

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json

import gspread
import pyspark.sql.functions as F
import pyspark.sql.types as T
from google.oauth2.service_account import Credentials

from maggulake.environment import DatabricksEnvironmentBuilder

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Environment and Widgets

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "atualiza_correcoes_manuais_from_sheets",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={"tab_name": ""},
)

spark = env.spark

catalog = env.settings.catalog
TAB_NAME = dbutils.widgets.get("tab_name").strip()
DEBUG = dbutils.widgets.get("debug") == "true"

POSTGRES_CORRECOES_MANUAIS_TABLE = "produtos_produtocorrecaomanual"

postgres = env.postgres_adapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Google Sheets Configuration

# COMMAND ----------

json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

gc = gspread.authorize(creds)

SHEET_ID = "1WZ2hBWPghV-co0fqrBP-HI78Wvzcwq6q_UOnZuU2OJU"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data from Google Sheets

# COMMAND ----------

spreadsheet = gc.open_by_key(SHEET_ID)

if TAB_NAME:
    tabs_to_process = [t.strip() for t in TAB_NAME.split(",") if t.strip()]
else:
    tabs_to_process = [ws.title for ws in spreadsheet.worksheets()]

all_rows = []
for tab in tabs_to_process:
    print(f"Reading tab: {tab}")
    rows = spreadsheet.worksheet(tab).get_all_records()
    for row in rows:
        row["ean"] = str(row["ean"]) if row["ean"] else None
        row["nome"] = str(row["nome"]) if row["nome"] else None
        row["marca"] = str(row["marca"]) if row["marca"] else None
        row["fabricante"] = str(row["fabricante"]) if row["fabricante"] else None
    all_rows.extend(rows)

print(f"Total rows read: {len(all_rows)}")

# COMMAND ----------

df_sheets = spark.createDataFrame(all_rows)

df_sheets = df_sheets.select(
    F.col("ean").cast(T.StringType()).alias("ean"),
    F.col("nome").cast(T.StringType()).alias("nome"),
    F.col("marca").cast(T.StringType()).alias("marca"),
    F.col("fabricante").cast(T.StringType()).alias("fabricante"),
)

df_sheets = df_sheets.filter(
    F.col("ean").isNotNull()
    & (F.col("ean") != "")
    & F.col("marca").isNotNull()
    & (F.col("marca") != "")
    & (F.col("marca") != "#N/A")
    & F.col("fabricante").isNotNull()
    & (F.col("fabricante") != "")
    & (F.col("fabricante") != "#N/A")
)

df_sheets = df_sheets.dropDuplicates(["ean"])

if DEBUG:
    df_sheets.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich with Product IDs

# COMMAND ----------

df_produtos_refined = (
    spark.read.table(f"{catalog}.refined.produtos_refined")
    .select(F.col("id").alias("produto_id"), F.col("ean"))
    .distinct()
)

df_final = df_sheets.join(df_produtos_refined, on="ean", how="left")

df_final = df_final.select(
    F.col("produto_id").alias("id"),
    F.col("ean"),
    F.col("marca"),
    F.col("fabricante"),
    F.current_timestamp().alias("criado_em"),
    F.current_timestamp().alias("atualizado_em"),
)

df_final = df_final.filter(F.col("id").isNotNull())
df_final = df_final.dropDuplicates(["ean"])

print(f"Records to upsert: {df_final.count()}")

if DEBUG:
    df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert to Postgres

# COMMAND ----------

postgres.upsert_into_table(
    df=df_final,
    table=POSTGRES_CORRECOES_MANUAIS_TABLE,
    conflict_columns=["ean"],
    update_columns=["marca", "fabricante", "atualizado_em"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

correcoes_atualizadas = postgres.read_table(spark, POSTGRES_CORRECOES_MANUAIS_TABLE)

ean_list = [row.ean for row in df_final.select("ean").collect()]

correcoes_atualizadas.filter(F.col("ean").isin(ean_list)).display()
