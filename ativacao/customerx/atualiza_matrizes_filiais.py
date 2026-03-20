# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Atualização Incremental de Matrizes e Filiais no CustomerX
# MAGIC
# MAGIC Este notebook atualiza clientes (matrizes/filiais) no CustomerX que foram modificados no Postgres.
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Incremental**: Atualiza apenas registros modificados desde última execução
# MAGIC - **Checkpoint-based**: Usa tabela `control.customerx_sync_checkpoint` para rastrear última sync
# MAGIC - **Update-only**: Apenas atualiza registros existentes (não cria novos)
# MAGIC - **Campos atualizados**: company_name, trading_name, custom_attributes
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC Execute `cria_matrizes_filiais.py` antes da primeira execução deste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from collections import Counter, defaultdict
from datetime import datetime

import requests
from pyspark.sql import functions as F

from maggulake.ativacao.customerx.checkpoint import get_last_sync, update_checkpoint
from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import ErrorAccumulator
from maggulake.integrations.customerx.models.conta import ContaDTO
from maggulake.integrations.customerx.models.loja import LojaDTO

# COMMAND ----------

dbutils.widgets.dropdown("validacao_estrita", "true", ["true", "false"])
# Importante para quando queremos atualizar a base inteira de uma vez
dbutils.widgets.dropdown("run_all", "false", ["false", "true"])

# COMMAND ----------

NOTEBOOK_NAME = "atualiza_matrizes_filiais"
env, customerx_client, dry_run = setup_customerx_notebook(dbutils, NOTEBOOK_NAME)

DEBUG = dbutils.widgets.get("debug") == "true"
RUN_ALL = dbutils.widgets.get("run_all") == "true"
VALIDACAO_ESTRITA = dbutils.widgets.get("validacao_estrita").lower() == "true"


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


# COMMAND ----------

# Obter última sincronização
last_sync = get_last_sync(spark, env.settings.catalog, NOTEBOOK_NAME)

if last_sync is None:
    print("⚠️  Primeira execução - processará TODOS os registros")
    # Define uma data muito antiga para capturar todos os registros
    last_sync = datetime(2000, 1, 1)
else:
    print(f"📅 Processando registros modificados após: {last_sync}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extração de Dados Modificados do Postgres

# COMMAND ----------

# Buscar contas modificadas usando adapter method
contas_df = env.postgres_replica_adapter.get_contas(spark)


# COMMAND ----------

# Buscar lojas modificadas usando adapter method
lojas_df = env.postgres_replica_adapter.get_lojas(
    spark,
    ativo=None,
    extra_fields=[
        "cnpj",
        "status",
        "endereco",
        "tamanho_loja",
        "cidade",
        "estado",
        "erp",
        "codigo_de_seis_digitos",
        "updated_at",
        "created_at",
    ],
)

# COMMAND ----------

if not RUN_ALL:
    contas_df = contas_df.filter(F.col("updated_at") > last_sync)
    lojas_df = lojas_df.filter(F.col("updated_at") > last_sync)
else:
    print("⚠️  Rodando tudo - processará TODOS os registros")

# COMMAND ----------

contas_raw = [row.asDict() for row in contas_df.collect()]
lojas_raw = [row.asDict() for row in lojas_df.collect()]


# COMMAND ----------

# Calcular estado mais comum por conta (para matrizes)
lojas_por_conta_estado = defaultdict(list)
for loja in lojas_raw:
    if loja.get('estado'):
        lojas_por_conta_estado[str(loja['conta_id'])].append(loja['estado'])

conta_estados = {
    conta_id: Counter(estados).most_common(1)[0][0]
    for conta_id, estados in lojas_por_conta_estado.items()
    if estados
}


# COMMAND ----------

contas = [
    ContaDTO.from_postgres_row(row, estado=conta_estados.get(str(row['id'])))
    for row in contas_raw
]
lojas = [LojaDTO.from_postgres_row(row) for row in lojas_raw]

print(
    f"📊 Registros modificados: {len(contas)} contas | {len(lojas)} lojas | {len(contas) + len(lojas)} total"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Buscar Clientes Existentes no CustomerX

# COMMAND ----------

existing_customers = customerx_client.fetch_all_customers(
    validacao_estrita=VALIDACAO_ESTRITA
)


# COMMAND ----------

# Todos os UUIDs de clientes presentes no CustomerX
existing_uuids = {c.external_id_client for c in existing_customers}

print(f"📋 CustomerX: {len(existing_customers)} clientes mapeados")
print(list(existing_uuids)[:10])


# COMMAND ----------


def get_id_customerx_by_postgres_uuid(
    postgres_uuid: str,
):
    """Retorna o ID do CustomerX correspondente ao UUID do Postgres."""

    uuid_to_id_customerx = {
        c.external_id_client: c.id_customerx for c in existing_customers
    }

    # print(f"🔢 UUIDs mapeados: {len(uuid_to_id_customerx)}")
    return uuid_to_id_customerx.get(postgres_uuid)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Identificar Contas e Lojas para Atualizar

# COMMAND ----------

# Apenas atualizar registros que já existem no CustomerX
contas_to_update = [conta for conta in contas if conta.postgres_uuid in existing_uuids]
lojas_to_update = [loja for loja in lojas if loja.postgres_uuid in existing_uuids]

contas_nao_encontradas = len(contas) - len(contas_to_update)
lojas_nao_encontradas = len(lojas) - len(lojas_to_update)

print(
    f"🔄 Para atualizar: {len(contas_to_update)} contas | {len(lojas_to_update)} lojas"
)
if contas_nao_encontradas > 0 or lojas_nao_encontradas > 0:
    # TODO: o que leva a termos essas contas nao encontradas.
    print(
        f"⚠️  Não encontrados no CX: {contas_nao_encontradas} contas | "
        f"{lojas_nao_encontradas} lojas (serão ignorados)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Atualizar Matrizes (Contas)

# COMMAND ----------


contas_atualizadas = 0
contas_falhadas = 0
contas_com_erro = []
error_acc = ErrorAccumulator(spark, env.settings.catalog, "atualiza_matrizes_filiais")

print(f"🏢 Atualizando matrizes ({len(contas_to_update)})...")


try:
    for conta in contas_to_update:
        id_customerx = get_id_customerx_by_postgres_uuid(conta.postgres_uuid)
        if not id_customerx:
            # Não encontrou o UUID no CustomerX
            continue

        # Preparar payload de atualização usando método DTO
        payload = conta.to_update_payload()

        if dry_run:
            contas_atualizadas += 1
            continue

        try:
            result = customerx_client.atualizar_cliente(id_customerx, payload)

            if result.get("id"):
                contas_atualizadas += 1
            else:
                contas_falhadas += 1

        except requests.RequestException as e:
            contas_falhadas += 1
            contas_com_erro.append((conta.nome, conta.postgres_uuid, str(e)))
            error_acc.add_error(
                operation_type="atualizar_matriz",
                entity_id=conta.postgres_uuid,
                entity_name=conta.nome,
                error_message=str(e),
            )
finally:
    # Não salva ainda pois temos mais um loop abaixo
    pass

print(f"✅ Matrizes: {contas_atualizadas} atualizadas | {contas_falhadas} falhadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Atualizar Filiais (Lojas)

# COMMAND ----------

lojas_atualizadas = 0
lojas_falhadas = 0
lojas_com_erro = []

print(f"🏪 Atualizando filiais ({len(lojas_to_update)})...")

try:
    for loja in lojas_to_update:
        id_customerx = get_id_customerx_by_postgres_uuid(loja.postgres_uuid)
        if not id_customerx:
            # Loja não encontrada no CustomerX
            continue

        payload = loja.to_update_payload()

        if dry_run:
            lojas_atualizadas += 1
            continue

        try:
            result = customerx_client.atualizar_cliente(id_customerx, payload)

            if result.get("id"):
                lojas_atualizadas += 1
            else:
                lojas_falhadas += 1

        except requests.RequestException as e:
            lojas_falhadas += 1
            lojas_com_erro.append((loja.nome, loja.postgres_uuid, str(e)))
            error_acc.add_error(
                operation_type="atualizar_filial",
                entity_id=loja.postgres_uuid,
                entity_name=loja.nome,
                error_message=str(e),
            )

finally:
    # Salva todos os erros acumulados (matrizes + filiais)
    error_acc.save_errors()

print(f"✅ Filiais: {lojas_atualizadas} atualizadas | {lojas_falhadas} falhadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Atualizar Checkpoint

# COMMAND ----------

total_processados = contas_atualizadas + lojas_atualizadas
total_falhados = contas_falhadas + lojas_falhadas

if total_falhados == 0:
    # Sucesso total - atualizar checkpoint
    update_checkpoint(
        spark,
        env.settings.catalog,
        NOTEBOOK_NAME,
        status="success",
        records_processed=total_processados,
    )
else:
    # Houve falhas - registrar mas não atualizar last_sync
    error_msg = f"Contas: {contas_falhadas} falhas | Lojas: {lojas_falhadas} falhas"
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
# MAGIC ## 10. Relatório Final

# COMMAND ----------

if contas_com_erro or lojas_com_erro:
    print("❌ Erros detectados:")
    if contas_com_erro:
        print(f"   Contas: {len(contas_com_erro)}")
        for nome, uuid, erro in contas_com_erro[:5]:  # Primeiros 5
            print(f"      - {nome}: {erro}")
    if lojas_com_erro:
        print(f"   Lojas: {len(lojas_com_erro)}")
        for nome, uuid, erro in lojas_com_erro[:5]:  # Primeiros 5
            print(f"      - {nome}: {erro}")
else:
    print("✅ Nenhum erro encontrado!")
