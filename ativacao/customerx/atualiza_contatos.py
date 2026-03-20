# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Atualização Incremental de Contatos no CustomerX
# MAGIC
# MAGIC Este notebook atualiza contatos (donos/gerentes) no CustomerX que foram modificados no Postgres.
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Incremental**: Atualiza apenas registros modificados desde última execução
# MAGIC - **Checkpoint-based**: Usa tabela `control.customerx_sync_checkpoint`
# MAGIC - **Update-only**: Apenas atualiza registros existentes (não cria novos)
# MAGIC - **Campos atualizados**: name, email, document (CPF), phones
# MAGIC - **Substituição de fakes**: Contatos fake são substituídos quando um dono/gerente real é encontrado
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC Execute `cria_contatos.py` antes da primeira execução deste notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from datetime import datetime

import requests
from pyspark.sql import functions as F

from maggulake.ativacao.customerx.checkpoint import get_last_sync, update_checkpoint
from maggulake.ativacao.customerx.constants import FAKE_CONTACT_TYPE
from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import ErrorAccumulator
from maggulake.integrations.customerx.models.contact import ContactDTO

# COMMAND ----------

dbutils.widgets.dropdown("run_all", "false", ["false", "true"])

# COMMAND ----------

NOTEBOOK_NAME = "atualiza_contatos"
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
# MAGIC ## 3. Extração de Dados Modificados do Postgres

# COMMAND ----------

# Buscar donos
donos_df = env.postgres_replica_adapter.get_donos(spark)

# Filtrar por data apenas se não for RUN_ALL
if not RUN_ALL:
    donos_df = donos_df.filter(F.col("updated_at") > last_sync)

if DEBUG:
    donos_df.limit(5).display()

# COMMAND ----------

# Buscar gerentes modificados usando adapter method + filtro por data
gerentes_df = env.postgres_replica_adapter.get_gerentes_lojas(spark)

# Filtrar apenas gerentes modificados após último sync
# TODO: essas tabelas não possuem um campo `atualizado_em` ou `updated_at`, então essa lógica de atualizaçõa não faz sentido. O que muito provavelmente vai acontecer por enquanto é atualizarmos sempre tudo e paciência. No futuro posso criar os campos `atualizado_em`
# gerentes_df = gerentes_df.filter(
#     (F.col("info_updated_at") > last_sync)
#     | (F.col("ativacao_updated_at") > last_sync)
#     | (F.col("atendente_updated_at") > last_sync)
# )


if DEBUG:
    gerentes_df.limit(5).display()

# COMMAND ----------

donos_raw = [row.asDict() for row in donos_df.collect()]
gerentes_raw = [row.asDict() for row in gerentes_df.collect()]

donos_postgres = [
    dto for row in donos_raw if (dto := ContactDTO.from_dono_row(row)) is not None
]
gerentes_postgres = [
    dto
    for row in gerentes_raw
    if (dto := ContactDTO.from_atendente_loja_row(row)) is not None
]

print(
    f"📊 Registros modificáveis: {len(donos_postgres)} donos | {len(gerentes_postgres)} gerentes"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Buscar Contatos Existentes no CustomerX

# COMMAND ----------

existing_contacts = customerx_client.fetch_all_contacts()

# COMMAND ----------

# Mapear (email, external_id_client) → contact_id do CX
email_client_to_id = {}
phone_client_to_id = {}

# Mapear external_id_client → contact_id para contatos fake
fake_contact_by_client: dict[str, int] = {}

for contact in existing_contacts:
    contact_id = contact.id_customerx
    external_id_client = contact.external_id_client

    if not contact_id or not external_id_client:
        continue

    # Mapear contatos fake por external_id_client
    if contact.tipo_contato["description"] == FAKE_CONTACT_TYPE:
        fake_contact_by_client[external_id_client] = contact_id

    # Mapear por email
    email = contact.email
    if email:
        key = (email.lower(), external_id_client)
        email_client_to_id[key] = contact_id

    # Mapear por telefone
    if contact.telefones:
        for phone in contact.telefones:
            number = phone.get("number")
            if number:
                key = (number, external_id_client)
                phone_client_to_id[key] = contact_id

print(f"\n📋 CustomerX: {len(existing_contacts)} contatos mapeados")
print(f"👻 Contatos fake encontrados: {len(fake_contact_by_client)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Identificar Contatos para Atualizar

# COMMAND ----------


def encontrar_contact_id(dto: ContactDTO) -> tuple[str | None, bool]:
    """Encontra o contact_id no CustomerX usando email, telefone ou contato fake.

    Returns:
        Tupla (contact_id, is_replacing_fake):
        - contact_id: ID do contato no CX, ou None se não encontrado
        - is_replacing_fake: True se o match foi via contato fake (substituição)
    """
    payload = dto.to_customerx_payload()
    external_id_client = dto.external_id_client

    # Tentar por email
    email = payload.get("email")
    if email:
        key = (email.lower(), external_id_client)
        if key in email_client_to_id:
            return email_client_to_id[key], False

    # Tentar por telefone
    for phone in payload.get("phones", []):
        number = phone.get("number")
        if number:
            key = (number, external_id_client)
            if key in phone_client_to_id:
                return phone_client_to_id[key], False

    # Tentar substituir contato fake do mesmo cliente
    if external_id_client in fake_contact_by_client:
        return fake_contact_by_client[external_id_client], True

    return None, False


# COMMAND ----------

# Encontrar contatos que já existem
donos_to_update: list[tuple[ContactDTO, str, bool]] = []
fakes_substituidos_por_donos = 0
for dono in donos_postgres:
    contact_id, is_fake = encontrar_contact_id(dono)
    if contact_id:
        donos_to_update.append((dono, contact_id, is_fake))
        if is_fake:
            fakes_substituidos_por_donos += 1
            # Remover do mapa de fakes para não ser substituído novamente
            fake_contact_by_client.pop(dono.external_id_client, None)

gerentes_to_update = []
fakes_substituidos_por_gerentes = 0
for gerente in gerentes_postgres:
    contact_id, is_fake = encontrar_contact_id(gerente)
    if contact_id:
        gerentes_to_update.append((gerente, contact_id, is_fake))
        if is_fake:
            fakes_substituidos_por_gerentes += 1
            fake_contact_by_client.pop(gerente.external_id_client, None)

print(
    f"🔄 Para atualizar: {len(donos_to_update)} donos | {len(gerentes_to_update)} gerentes"
)
print(
    f"👻 Fakes a substituir: {fakes_substituidos_por_donos} por donos | "
    f"{fakes_substituidos_por_gerentes} por gerentes"
)
# TODO: por que isso acontece e como posso resolver?
print(
    f"⚠️  Não encontrados: {len(donos_postgres) - len(donos_to_update)} donos | "
    f"{len(gerentes_postgres) - len(gerentes_to_update)} gerentes (serão ignorados)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Atualizar Donos

# COMMAND ----------

donos_atualizados = 0
donos_falhados = 0
donos_com_erro = []
error_acc = ErrorAccumulator(spark, env.settings.catalog, "atualiza_contatos")

print(f"👔 Atualizando donos ({len(donos_to_update)})...")

try:
    for dono, contact_id, is_replacing_fake in donos_to_update:
        if dry_run:
            donos_atualizados += 1
            continue

        # Ao substituir fake, envia payload completo (incluindo external_id_client)
        payload = (
            dono.to_customerx_payload()
            if is_replacing_fake
            else dono.to_update_payload()
        )

        try:
            result = customerx_client.atualizar_contato(contact_id, payload)

            if result.get("id"):
                donos_atualizados += 1
                print(f"✅ {dono.nome}")
            else:
                donos_falhados += 1

        except requests.RequestException as e:
            donos_falhados += 1
            error_msg = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_msg = f"[{e.response.status_code}] {e.response.text[:200]}"
            donos_com_erro.append((dono.nome, contact_id, error_msg))
            error_acc.add_error(
                operation_type="atualizar_contato",
                entity_id=contact_id,
                entity_name=dono.nome,
                error_message=f"{error_msg} | Payload: {payload}",
            )
finally:
    # Não salva ainda pois temos mais um loop abaixo
    pass

print(f"✅ Donos: {donos_atualizados} atualizados | {donos_falhados} falhados")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Atualizar Gerentes

# COMMAND ----------

gerentes_atualizados = 0
gerentes_falhados = 0
gerentes_com_erro = []

print(f"👨‍💼 Atualizando gerentes ({len(gerentes_to_update)})...")

try:
    for gerente, contact_id, is_replacing_fake in gerentes_to_update:
        if dry_run:
            gerentes_atualizados += 1
            continue

        # Ao substituir fake, envia payload completo (incluindo external_id_client)
        payload = (
            gerente.to_customerx_payload()
            if is_replacing_fake
            else gerente.to_update_payload()
        )

        try:
            result = customerx_client.atualizar_contato(contact_id, payload)

            if result.get("id"):
                gerentes_atualizados += 1
                print(f"✅ {gerente.nome}")
            else:
                gerentes_falhados += 1

        except requests.RequestException as e:
            gerentes_falhados += 1
            error_msg = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_msg = f"[{e.response.status_code}] {e.response.text[:200]}"
            gerentes_com_erro.append((gerente.nome, contact_id, error_msg))
            error_acc.add_error(
                operation_type="atualizar_contato",
                entity_id=contact_id,
                entity_name=gerente.nome,
                error_message=f"{error_msg} | Payload: {payload}",
            )
finally:
    # Salva todos os erros acumulados (donos + gerentes)
    error_acc.save_errors()

print(
    f"\n✅ Gerentes: {gerentes_atualizados} atualizados | {gerentes_falhados} falhados"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Atualizar Checkpoint

# COMMAND ----------

total_processados = donos_atualizados + gerentes_atualizados
total_falhados = donos_falhados + gerentes_falhados

if total_falhados == 0:
    update_checkpoint(
        spark,
        env.settings.catalog,
        NOTEBOOK_NAME,
        status="success",
        records_processed=total_processados,
    )
else:
    update_checkpoint(
        spark,
        env.settings.catalog,
        NOTEBOOK_NAME,
        status="failed",
        records_processed=total_processados,
        error_message=f"Donos: {donos_falhados} falhas | Gerentes: {gerentes_falhados} falhas",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Relatório Final
# MAGIC

# COMMAND ----------

if donos_com_erro or gerentes_com_erro:
    print("❌ Erros detectados:")
    if donos_com_erro:
        print(f"   Donos: {len(donos_com_erro)}")
        for nome, cid, erro in donos_com_erro[:5]:
            print(f"      - {nome} (ID:{cid}): {erro}")
    if gerentes_com_erro:
        print(f"   Gerentes: {len(gerentes_com_erro)}")
        for nome, cid, erro in gerentes_com_erro[:5]:
            print(f"      - {nome} (ID:{cid}): {erro}")
else:
    print("✅ Nenhum erro encontrado!")
