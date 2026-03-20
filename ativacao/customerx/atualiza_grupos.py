# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Atualização Incremental de Grupos no CustomerX
# MAGIC
# MAGIC Este notebook atualiza grupos no CustomerX que foram modificados no Postgres.
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Incremental**: Atualiza apenas registros modificados desde última execução
# MAGIC - **Checkpoint-based**: Usa tabela `control.customerx_sync_checkpoint`
# MAGIC - **Update-only**: Apenas atualiza registros existentes (não cria novos)
# MAGIC - **Campos atualizados**: description (nome do grupo), status
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC Execute `cria_grupos.py` antes da primeira execução deste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from datetime import datetime

import requests
from pyspark.sql import functions as F

from maggulake.ativacao.customerx.checkpoint import get_last_sync, update_checkpoint
from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import (
    ErrorAccumulator,
)
from maggulake.integrations.customerx.models.group import GroupDTO

# COMMAND ----------

dbutils.widgets.dropdown("run_all", "false", ["false", "true"])

# COMMAND ----------

NOTEBOOK_NAME = "atualiza_grupos"

env, customerx_client, dry_run = setup_customerx_notebook(dbutils, NOTEBOOK_NAME)

DEBUG = dbutils.widgets.get("debug") == "true"
RUN_ALL = dbutils.widgets.get("run_all") == "true"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar Checkpoint e Determinar Período

# COMMAND ----------

# Registrar início da execução
update_checkpoint(
    spark,
    env.settings.catalog,
    NOTEBOOK_NAME,
    status="running",
)

# Obter última sincronização
last_sync = get_last_sync(spark, env.settings.catalog, NOTEBOOK_NAME)

if last_sync is None:
    print("⚠️  Primeira execução - processará TODOS os registros")
    last_sync = datetime(2000, 1, 1)
else:
    print(f"📅 Processando registros modificados após: {last_sync}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extração de Contas Modificadas do Postgres

# COMMAND ----------

# Buscar contas usando adapter method + filtrar por data
contas_df = env.postgres_replica_adapter.get_contas(spark)

if not RUN_ALL:
    # Filtrar apenas contas modificadas após último sync
    contas_df = contas_df.filter(F.col("updated_at") > last_sync)

contas_raw = [row.asDict() for row in contas_df.collect()]

print(f"Contas a serem processadas: {len(contas_raw)}")

# COMMAND ----------

grupos_to_sync = [
    GroupDTO(
        external_id=str(conta["id"]),
        description=conta["name"],
        status=True,
    )
    for conta in contas_raw
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Buscar Grupos Existentes no CustomerX

# COMMAND ----------

existing_groups = customerx_client.fetch_all_groups()

# COMMAND ----------

# Mapear external_id → group_id do CX
external_id_to_group_id = {group.external_id: group.id for group in existing_groups}

print(f"📋 CustomerX: {len(existing_groups)} grupos mapeados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Identificar Grupos para Atualizar

# COMMAND ----------

grupos_to_update: list[tuple[GroupDTO, int]] = []

for grupo in grupos_to_sync:
    group_id = external_id_to_group_id.get(grupo.external_id)
    if group_id:
        grupos_to_update.append((grupo, group_id))

print(f"🔄 Para atualizar: {len(grupos_to_update)} grupos")
# TODO: o que leva um grupo a nao ser encontrado? O que eu poderia fazer para resolver? Como debugar esse grupo?
print(
    f"⚠️  Não encontrados: {len(grupos_to_sync) - len(grupos_to_update)} grupos (serão ignorados)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Atualizar Grupos

# COMMAND ----------

grupos_atualizados = 0
grupos_falhados = 0
grupos_com_erro = []
error_acc = ErrorAccumulator(spark, env.settings.catalog, "atualiza_grupos")

print(f"🏢 Atualizando grupos ({len(grupos_to_update)})...")

try:
    for grupo, group_id in grupos_to_update:
        if dry_run:
            grupos_atualizados += 1
            continue

        payload = grupo.to_customerx_payload()

        try:
            result = customerx_client.atualizar_grupo(group_id, payload)

            if result.get("id"):
                grupos_atualizados += 1
            else:
                grupos_falhados += 1

        except requests.RequestException as e:
            grupos_falhados += 1
            grupos_com_erro.append((grupo.description, grupo.external_id, str(e)))
            error_acc.add_error(
                operation_type="atualizar_grupo",
                entity_id=grupo.external_id,
                entity_name=grupo.description,
                error_message=str(e),
            )
finally:
    error_acc.save_errors()

print(f"✅ Grupos: {grupos_atualizados} atualizados | {grupos_falhados} falhados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Atualizar Checkpoint

# COMMAND ----------

total_processados = grupos_atualizados

if grupos_falhados == 0:
    update_checkpoint(
        spark,
        env.settings.catalog,
        NOTEBOOK_NAME,
        status="success",
        records_processed=total_processados,
    )
else:
    error_msg = f"Grupos: {grupos_falhados} falhas"
    update_checkpoint(
        spark,
        env.settings.catalog,
        NOTEBOOK_NAME,
        status="failed",
        records_processed=total_processados,
        error_message=error_msg,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Relatório Final


# COMMAND ----------

if grupos_com_erro:
    print("❌ Erros detectados:")
    print(f"   Grupos: {len(grupos_com_erro)}")
    for nome, external_id, erro in grupos_com_erro[:5]:
        print(f"      - {nome} ({external_id}): {erro}")
else:
    print("✅ Nenhum erro encontrado!")
