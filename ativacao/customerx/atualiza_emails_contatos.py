# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Atualização de Emails de Contatos no CustomerX
# MAGIC
# MAGIC Este notebook atualiza emails de contatos no CustomerX, substituindo emails fake
# MAGIC (`loja_{uuid}@maggu.ai`) por emails reais quando gerentes cadastram seus dados.
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Busca emails fake**: Identifica contatos com email `loja_{uuid}@maggu.ai`
# MAGIC - **Cruza com Postgres**: Verifica se gerente agora tem email real cadastrado
# MAGIC - **Atualiza seletivamente**: Substitui apenas emails fake → real
# MAGIC - **Idempotente**: Pode ser executado múltiplas vezes sem efeitos colaterais
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC Execute `cria_contatos.py` antes deste.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import requests

from maggulake.ativacao.customerx.constants import FAKE_EMAIL_DOMAIN
from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import (
    ErrorAccumulator,
)
from maggulake.integrations.customerx.models.contact import ContactDTO

# COMMAND ----------

NOTEBOOK_NAME = "atualiza_emails_contatos"
env, customerx_client, dry_run = setup_customerx_notebook(dbutils, NOTEBOOK_NAME)


DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Buscar Contatos com Email Fake

# COMMAND ----------

existing_contacts = customerx_client.fetch_all_contacts()

# COMMAND ----------

contacts_with_fake_email = [
    contact
    for contact in existing_contacts
    if contact.email
    and contact.email.endswith(f"@{FAKE_EMAIL_DOMAIN}")
    and contact.tipo_contato == "Gerente"
]

print(
    f"📋 Total contatos: {len(existing_contacts)} | Com email fake: {len(contacts_with_fake_email)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Buscar Gerentes com Email Real no Postgres

# COMMAND ----------

# Buscar gerentes usando adapter method
gerentes_df = env.postgres_replica_adapter.get_gerentes_lojas(spark)
gerentes_raw = [row.asDict() for row in gerentes_df.collect()]

gerentes_com_email_real = [
    dto
    for row in gerentes_raw
    if (dto := ContactDTO.from_atendente_loja_row(row)) is not None
    and not dto.email.endswith(f"@{FAKE_EMAIL_DOMAIN}")
]

print(f"📊 Gerentes com email real: {len(gerentes_com_email_real)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identificar Contatos para Atualizar

# COMMAND ----------

fake_contacts_by_loja = {
    contact.external_id_client: contact
    for contact in contacts_with_fake_email
    if contact.external_id_client
}

contatos_para_atualizar: list[tuple[ContactDTO, int]] = []

for gerente in gerentes_com_email_real:
    loja_id = gerente.external_id_client
    if loja_id in fake_contacts_by_loja:
        fake_contact = fake_contacts_by_loja[loja_id]
        contatos_para_atualizar.append((gerente, fake_contact.id_customerx))

print(f"🔄 Contatos para atualizar: {len(contatos_para_atualizar)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Atualizar Emails (Fake → Real)

# COMMAND ----------

atualizados = 0
erros = 0
error_acc = ErrorAccumulator(spark, env.settings.catalog, "atualiza_emails_contatos")

print(f"📧 Atualizando emails ({len(contatos_para_atualizar)})...")

try:
    for gerente, contact_id in contatos_para_atualizar:
        try:
            payload = gerente.to_update_payload()

            if not dry_run:
                customerx_client.atualizar_contato(
                    contact_id=contact_id, payload=payload
                )
                atualizados += 1
                print(f"✅ Email atualizado: {gerente.nome} ({gerente.email})")

        except requests.RequestException as e:
            erros += 1
            error_acc.add_error(
                operation_type="atualizar_email",
                entity_id=contact_id,
                entity_name=gerente.nome,
                error_message=str(e),
            )
finally:
    error_acc.save_errors()

print(f"✅ Atualizações: {atualizados} sucesso | {erros} erros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumo Final


# COMMAND ----------

if len(contacts_with_fake_email) > 0:
    taxa = atualizados / len(contacts_with_fake_email) * 100
    restantes = len(contacts_with_fake_email) - atualizados
    print(f"📊 Qualidade: {taxa:.1f}% atualizados | {restantes} emails fake restantes")
else:
    print("✅ Todos os contatos têm emails reais!")
