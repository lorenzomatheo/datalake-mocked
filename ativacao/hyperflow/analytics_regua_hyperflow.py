# Databricks notebook source
# MAGIC %md
# MAGIC # Analytics: Régua de Comunicação Hyperflow
# MAGIC
# MAGIC Este notebook calcula e persiste views de analytics sobre o envio de mensagens via Hyperflow
# MAGIC para a régua de comunicação de atendentes.
# MAGIC
# MAGIC **Fontes:**
# MAGIC - `raw.regua_atendente_mensagens_enviadas` — histórico de mensagens enviadas com sucesso
# MAGIC - `raw.regua_atendente_mensagens_enviadas_erros` — histórico de erros ao tentar enviar
# MAGIC - PostgreSQL — dados de atendentes (opt-out, telefone, loja, rede)
# MAGIC
# MAGIC **Tabelas geradas** (schema `{catalog}.analytics`):
# MAGIC - `hyperflow_regua_alcance` — snapshot KPI: atendentes únicos alcançados vs elegíveis
# MAGIC - `hyperflow_regua_opt_out` — atendentes que saíram do canal (opt-out)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from datetime import datetime, timezone

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from maggulake.environment import DatabricksEnvironmentBuilder
from maggulake.environment.tables import Table
from maggulake.schemas import (
    schema_hyperflow_regua_alcance,
    schema_hyperflow_regua_opt_out,
)

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build("analytics_regua_hyperflow", dbutils)
CATALOG = env.settings.catalog
postgres = env.postgres_replica_adapter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constantes

# COMMAND ----------

TIPO_ERRO_TELEFONE_INVALIDO = "NumeroTelefoneInvalidoError"

# COMMAND ----------


def salvar_tabela_delta(
    dataframe: DataFrame,
    tabela: Table,
    mode: str = "overwrite",
    sufixo_log: str | None = None,
) -> None:
    dataframe.write.format("delta").mode(mode).saveAsTable(tabela.value)
    mensagem = f"✓ Tabela salva: {CATALOG}.{tabela.value}"
    if sufixo_log:
        mensagem += f" ({sufixo_log})"
    print(mensagem)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Criacao das tabelas (se nao existirem)

# COMMAND ----------

env.create_table_if_not_exists(
    Table.hyperflow_regua_alcance, schema_hyperflow_regua_alcance
)
env.create_table_if_not_exists(
    Table.hyperflow_regua_opt_out, schema_hyperflow_regua_opt_out
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura das fontes base

# COMMAND ----------

df_msgs = env.table(Table.regua_atendente_mensagens_enviadas)
df_erros = env.table(Table.regua_atendente_mensagens_enviadas_erros)

# Trabalhar apenas com dados reais (não testes)
df_msgs_prod = df_msgs.filter(F.col("eh_somente_teste") == F.lit(False))
df_erros_prod = df_erros.filter(F.col("eh_somente_teste") == F.lit(False))

# NOTE: esse notebook analisa todas as msgs enviadas pelo dfatabricks, mas isso é otimista.
# A planilha https://docs.google.com/spreadsheets/d/1MLwfpCHBubUjlagRJl3NWCeNYDMw8kD6hd3HgfENqY4/edit?gid=1704984708#gid=1704984708 registra todas as msgs que o hyperflow conseguiu de fato enviar
# TODO: incorporar essa planilha depois

total_mensagens_prod = df_msgs_prod.count()
total_erros_prod = df_erros_prod.count()
print(f"Mensagens enviadas: {total_mensagens_prod}")
print(f"Erros de envio: {total_erros_prod}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura PostgreSQL — atendentes elegíveis (opt-in)

# COMMAND ----------

df_atendentes_pg = postgres.get_atendentes_com_lojas(
    apenas_ativos=True,
    apenas_com_vendas_ultimos_30_dias=True,  # Exclui atendentes fantasmas que nunca venderam nada
)

# COMMAND ----------

# Quem estiver como "pre-cadastro" não pode receber mensagens
df_atendentes_pg = df_atendentes_pg[
    df_atendentes_pg["status_ativacao"] == "cadastrado"
].reset_index(drop=True)

df_atendentes_spark = spark.createDataFrame(df_atendentes_pg)

# COMMAND ----------

total_atendentes_cadastrados = df_atendentes_spark.count()
print(f"Atendentes cadastrados no banco: {total_atendentes_cadastrados}")

total_elegiveis_opt_in = int(
    df_atendentes_pg["deve_receber_comunicacoes_no_whatsapp"].sum()
)
print(f"Elegíveis opt-in (deve_receber_comunicacoes): {total_elegiveis_opt_in}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Métricas mínimas para persistência

# COMMAND ----------

total_alcancados = df_msgs_prod.select("id_atendente").distinct().count()
percentual_alcance = round(
    (total_alcancados / total_elegiveis_opt_in * 100)
    if total_elegiveis_opt_in > 0
    else 0.0,
    2,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persiste: hyperflow_regua_alcance
# MAGIC
# MAGIC Snapshot KPI: quantos atendentes únicos já foram alcançados vs total elegíveis.

# COMMAND ----------

df_alcance = spark.range(1).select(
    F.lit(datetime.now(timezone.utc)).cast("timestamp").alias("data_snapshot"),
    F.lit(total_alcancados).cast("int").alias("total_atendentes_alcancados"),
    F.lit(total_elegiveis_opt_in).cast("int").alias("total_elegiveis_opt_in"),
    F.lit(total_atendentes_cadastrados).cast("int").alias("total_cadastrados"),
    F.lit(percentual_alcance).cast("float").alias("percentual_alcance"),
    F.current_timestamp().alias("atualizado_em"),
)

salvar_tabela_delta(
    df_alcance,
    Table.hyperflow_regua_alcance,
    mode="append",
    sufixo_log=(
        f"Alcance atual: {percentual_alcance:.2f}% "
        f"({total_alcancados}/{total_elegiveis_opt_in})"
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persiste: hyperflow_regua_opt_out
# MAGIC
# MAGIC Atendentes que saíram do canal (opt-out): `deve_receber_comunicacoes_no_whatsapp = False`.

# COMMAND ----------

df_opt_out_pg = df_atendentes_pg[
    ~df_atendentes_pg["deve_receber_comunicacoes_no_whatsapp"]
].copy()

df_opt_out = spark.createDataFrame(
    df_opt_out_pg[
        [
            "id_atendente",
            "username",
            "nome_atendente",
            "telefone_atendente",
            "id_loja",
            "nome_loja",
            "nome_rede",
        ]
    ]
)

# Enriquecer: quantas mensagens recebeu antes de dar opt-out
df_msgs_count = df_msgs_prod.groupBy("id_atendente").agg(
    F.count("id").alias("mensagens_recebidas_antes_opt_out"),
    F.max("data_hora_envio").alias("ultima_mensagem_antes_opt_out"),
)

df_opt_out = df_opt_out.join(df_msgs_count, on="id_atendente", how="left")
df_opt_out = df_opt_out.withColumn("atualizado_em", F.current_timestamp()).withColumn(
    "id_atendente", F.col("id_atendente").cast("int")
)


total_opt_out = df_opt_out.count()
salvar_tabela_delta(
    df_opt_out,
    Table.hyperflow_regua_opt_out,
    sufixo_log=f"{total_opt_out} opt-outs",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumo final

# COMMAND ----------

nao_alcancados = max(total_elegiveis_opt_in - total_alcancados, 0)

df_erros_resumo = (
    df_erros_prod.groupBy("tipo_erro", "subtipo_erro")
    .agg(F.count("id").alias("total_erros"))
    .orderBy(F.col("total_erros").desc())
)
total_tipos_erro = df_erros_resumo.count()
total_erros_telefone_invalido = df_erros_prod.filter(
    F.col("tipo_erro") == TIPO_ERRO_TELEFONE_INVALIDO
).count()

row_periodo = df_msgs_prod.agg(
    F.min("data_hora_envio").alias("primeiro_envio"),
    F.max("data_hora_envio").alias("ultimo_envio"),
).first()
primeiro_envio = row_periodo["primeiro_envio"]
ultimo_envio = row_periodo["ultimo_envio"]

tipos_mensagem = [
    row["tipo_mensagem"]
    for row in df_msgs_prod.select("tipo_mensagem").distinct().collect()
]

top_erros = df_erros_resumo.select("tipo_erro", "subtipo_erro", "total_erros").take(3)

taxa_erro = round(
    (total_erros_prod / (total_mensagens_prod + total_erros_prod) * 100)
    if (total_mensagens_prod + total_erros_prod) > 0
    else 0.0,
    2,
)
media_msgs_por_atendente = round(
    total_mensagens_prod / total_alcancados if total_alcancados > 0 else 0.0, 1
)

print("=" * 60)
print("RESUMO — Analytics Régua Hyperflow")
print("=" * 60)

print("\n── Volume ──")
print(f"  Mensagens enviadas (produção): {total_mensagens_prod:,}")
print(f"  Erros de envio (produção):     {total_erros_prod:,}")
print(f"  Taxa de erro:                  {taxa_erro:.2f}%")
print(f"  Período coberto:               {primeiro_envio} → {ultimo_envio}")

print("\n── Tipos de mensagem ──")
for t in sorted(tipos_mensagem):
    print(f"  • {t}")

print("\n── Alcance ──")
print(f"  Atendentes cadastrados no banco:   {total_atendentes_cadastrados:,}")
print(f"  Elegíveis opt-in:                  {total_elegiveis_opt_in:,}")
print(f"  Alcançados (receberam ≥1 msg):     {total_alcancados:,}")
print(f"  Ainda não alcançados:              {nao_alcancados:,}")
print(
    f"  Percentual de alcance:             {percentual_alcance:.2f}%"
    f" ({total_alcancados}/{total_elegiveis_opt_in})"
)
print(f"  Média de msgs por atendente:       {media_msgs_por_atendente}")

print("\n── Saúde do canal ──")
print(f"  Opt-outs:                  {total_opt_out:,}")
print(f"  Erros de telefone inválido: {total_erros_telefone_invalido:,}")
print(f"  Tipos de erro distintos:   {total_tipos_erro}")

if top_erros:
    print("\n── Top erros ──")
    for row in top_erros:
        print(
            f"  • {row['tipo_erro']} / {row['subtipo_erro']}: "
            f"{row['total_erros']:,} ocorrências"
        )

print("\n" + "=" * 60)
