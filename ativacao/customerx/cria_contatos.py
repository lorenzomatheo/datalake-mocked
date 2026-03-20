# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Criação de Contatos no CustomerX
# MAGIC
# MAGIC Este notebook realiza a criação de contatos no CustomerX a partir do Postgres:
# MAGIC - **Donos de Contas**: Tabela `contas_donoconta` vai gerar contatos tipo "Dono"
# MAGIC - **Gerentes de Lojas**: JOIN `atendentes_atendenteloja` onde os atendentes tenha cargo "GERENTE" vai gerar contatos tipo "Gerente"
# MAGIC - **Fake (placeholder)**: Contato automático para clientes que ficaram sem nenhum contato real
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Three-phase**: Cria donos, depois gerentes, depois contatos fake para clientes órfãos
# MAGIC - **Create-only**: Não atualiza registros existentes
# MAGIC - **Idempotente**: Compara com contatos existentes para evitar duplicação
# MAGIC - **Validação**: Apenas cria contatos com nome válido
# MAGIC - **Fallback de E-mail**: Gerentes sem email recebem `loja_{uuid}@maggu.ai` para satisfazer requisito da API
# MAGIC - **Um gerente por loja**: Se houver múltiplos gerentes ativos, seleciona o primeiro cadastrado
# MAGIC - **Fake contacts**: Clientes sem nenhum contato real recebem um contato fake para garantir que automações funcionem
# MAGIC
# MAGIC ## Pré-requisito
# MAGIC Execute o notebook `cria_matrizes_filiais.py` antes deste.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

import requests

from maggulake.ativacao.customerx.constants import FAKE_EMAIL_DOMAIN
from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import ErrorAccumulator
from maggulake.integrations.customerx.models.contact import ContactDTO

# COMMAND ----------

env, customerx_client, dry_run = setup_customerx_notebook(dbutils, "cria_contatos")

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extração de Dados do Postgres

# COMMAND ----------

donos_df = env.postgres_replica_adapter.read_table(spark, table="contas_donoconta")
gerentes_df = env.postgres_replica_adapter.get_gerentes_lojas(spark)

# COMMAND ----------

donos_raw = [row.asDict() for row in donos_df.collect()]
gerentes_raw = [row.asDict() for row in gerentes_df.collect()]

if DEBUG:
    print(f"📊 Donos: {len(donos_raw)} | Gerentes: {len(gerentes_raw)}\n")

    print(donos_raw[0], "\n")
    print(gerentes_raw[0])


# COMMAND ----------

donos_postgres = [
    dto for row in donos_raw if (dto := ContactDTO.from_dono_row(row)) is not None
]
gerentes_postgres = [
    dto
    for row in gerentes_raw
    if (dto := ContactDTO.from_atendente_loja_row(row)) is not None
]

gerentes_com_email_real = sum(
    1 for dto in gerentes_postgres if not dto.email.endswith(f"@{FAKE_EMAIL_DOMAIN}")
)

if DEBUG:
    print(
        f"📧 Gerentes: {gerentes_com_email_real} email real, {len(gerentes_postgres) - gerentes_com_email_real} fallback",
        "\n",
    )

    print("primeiro dono:\n", donos_postgres[0], "\n")
    print("primeiro gerente:\n", gerentes_postgres[0], "\n")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Comparar com Contatos Existentes

# COMMAND ----------


def contato_ja_existe(
    dto: ContactDTO,
    existing_emails: set[str],
    existing_phone_client_pairs: set[tuple[str, str]],
) -> bool:
    """Verifica se contato já existe.

    A API CustomerX valida unicidade de email globalmente (não permite email duplicado
    em nenhum contato). Telefones podem se repetir entre contatos diferentes.
    """
    payload = dto.to_customerx_payload()

    # Verificar se email já existe (email é único globalmente na API)
    email = payload.get("email")

    if email and email.lower() in existing_emails:
        return True

    # Verificar se telefone E external_id_client já existem juntos
    # (mesmo telefone pode existir para clientes diferentes)
    external_id_client = dto.external_id_client
    for phone in payload.get("phones", []):
        number = phone.get("number")
        if number and (number, external_id_client) in existing_phone_client_pairs:
            return True

    return False


# COMMAND ----------

existing_contacts = customerx_client.fetch_all_contacts()

if DEBUG:
    print(f"🔍 Contatos existentes: {len(existing_contacts)}")
    if existing_contacts:
        print(f"   Exemplo: {existing_contacts[0].nome} | {existing_contacts[0].email}")

# COMMAND ----------

existing_emails = []
existing_phone_client_pairs = set()

for contact in existing_contacts:
    # Email é único globalmente (não pode repetir em nenhum contato)
    email = contact.email
    if email:
        existing_emails.append(email.lower())

    # Telefone + external_id_client devem ser únicos juntos
    external_id_client = contact.external_id_client
    if not external_id_client:
        continue

    if contact.telefones:
        for phone in contact.telefones:
            number = phone.get("number")
            if number:
                existing_phone_client_pairs.add((number, external_id_client))

existing_emails = set(existing_emails)
existing_phone_client_pairs = set(existing_phone_client_pairs)

if DEBUG:
    print(f"   Emails únicos: {len(existing_emails)}")
    print(f"   Telefone+Cliente únicos: {len(existing_phone_client_pairs)}")

donos_to_create = [
    dto
    for dto in donos_postgres
    if not contato_ja_existe(dto, existing_emails, existing_phone_client_pairs)
]
gerentes_to_create = [
    dto
    for dto in gerentes_postgres
    if not contato_ja_existe(dto, existing_emails, existing_phone_client_pairs)
]

print(
    f"🔍 Contatos a criar: Donos {len(donos_to_create)}/{len(donos_postgres)} | "
    f"Gerentes {len(gerentes_to_create)}/{len(gerentes_postgres)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Contatos - Fase 1: Donos

# COMMAND ----------

donos_criados = 0
donos_erros = 0
error_acc = ErrorAccumulator(spark, env.settings.catalog, "cria_contatos")

print(f"👔 Criando donos ({len(donos_to_create)})...")

try:
    for dono in donos_to_create:
        if dry_run:
            donos_criados += 1
            if DEBUG:
                print(f"   [DRY RUN] {dono.nome}")
            continue

        payload = dono.to_customerx_payload()

        try:
            customerx_client.criar_contato(payload)
            donos_criados += 1
            if DEBUG:
                print(f"   ✅ {dono.nome}")
        except requests.RequestException as e:
            donos_erros += 1
            error_msg = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_msg = f"[{e.response.status_code}] {e.response.text[:200]}"
            if DEBUG:
                print(f"   ❌ {dono.nome}: {error_msg}")
            error_acc.add_error(
                operation_type="criar_contato",
                entity_id=dono.external_id_client,
                entity_name=dono.nome,
                error_message=f"{error_msg} | Payload: {payload}",
            )
finally:
    # Não salva ainda pois temos mais um loop abaixo
    pass

print(f"✅ Donos: {donos_criados} criados | {donos_erros} erros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Criar Contatos - Fase 2: Gerentes

# COMMAND ----------

gerentes_criados = 0
gerentes_erros = 0

print(f"👨‍💼 Criando gerentes ({len(gerentes_to_create)})...")

try:
    for gerente in gerentes_to_create:
        if dry_run:
            gerentes_criados += 1
            if DEBUG:
                print(f"   [DRY RUN] {gerente.nome}")
            continue

        payload = gerente.to_customerx_payload()

        try:
            customerx_client.criar_contato(payload)
            gerentes_criados += 1
            if DEBUG:
                print(f"   ✅ {gerente.nome}")
        except requests.RequestException as e:
            gerentes_erros += 1
            error_msg = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_msg = f"[{e.response.status_code}] {e.response.text[:200]}"
            if DEBUG:
                print(f"   ❌ {gerente.nome}: {error_msg}")
            error_acc.add_error(
                operation_type="criar_contato",
                entity_id=gerente.external_id_client,
                entity_name=gerente.nome,
                error_message=f"{error_msg} | Payload: {payload}",
            )
finally:
    # Salva todos os erros acumulados (donos + gerentes)
    error_acc.save_errors()

print(f"\n✅ Gerentes: {gerentes_criados} criados | {gerentes_erros} erros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criar Contatos - Fase 3: Contatos Fake para Clientes Órfãos

# COMMAND ----------

# MAGIC %md
# MAGIC Clientes (matrizes e filiais) sem nenhum contato vinculado precisam de um contato
# MAGIC fake para que as automações do CustomerX funcionem corretamente.
# MAGIC
# MAGIC Estratégia:
# MAGIC 1. Re-buscar todos os contatos do CX (incluindo os recém-criados nas fases 1 e 2)
# MAGIC 2. Buscar todos os clientes do CX
# MAGIC 3. Identificar clientes sem nenhum contato
# MAGIC 4. Criar contato fake para cada um

# COMMAND ----------

# Re-buscar contatos após criação de donos e gerentes
all_contacts_after = customerx_client.fetch_all_contacts()

# Coletar external_id_client de todos os contatos existentes
clients_with_contacts = {
    c.external_id_client for c in all_contacts_after if c.external_id_client
}

print(f"📋 Contatos após fases 1 e 2: {len(all_contacts_after)}")
print(f"📋 Clientes com pelo menos 1 contato: {len(clients_with_contacts)}")

# COMMAND ----------

all_cx_customers = customerx_client.fetch_all_customers()

# Identificar clientes sem contato
clients_without_contacts = [
    customer
    for customer in all_cx_customers
    if customer.external_id_client
    and customer.external_id_client not in clients_with_contacts
]

print(
    f"🔍 Clientes sem contato: {len(clients_without_contacts)}/{len(all_cx_customers)}"
)

# COMMAND ----------

# Criar DTOs fake para clientes órfãos
fakes_to_create = [
    ContactDTO.from_fake_client(
        external_id=customer.external_id_client,
        client_name=customer.nome_fantasia or customer.nome_empresa,
    )
    for customer in clients_without_contacts
]

# Filtrar por email já existente (idempotência)
existing_emails_after = {c.email.lower() for c in all_contacts_after if c.email}
fakes_to_create = [
    f for f in fakes_to_create if f.email.lower() not in existing_emails_after
]

print(f"👻 Contatos fake a criar: {len(fakes_to_create)}")

# COMMAND ----------

fakes_criados = 0
fakes_erros = 0
error_acc_fakes = ErrorAccumulator(spark, env.settings.catalog, "cria_contatos_fake")

print(f"👻 Criando contatos fake ({len(fakes_to_create)})...")

try:
    for fake in fakes_to_create:
        if dry_run:
            fakes_criados += 1
            if DEBUG:
                print(f"   [DRY RUN] {fake.nome} → {fake.email}")
            continue

        payload = fake.to_customerx_payload()

        try:
            customerx_client.criar_contato(payload)
            fakes_criados += 1
            if DEBUG:
                print(f"   ✅ {fake.nome}")
        except requests.RequestException as e:
            fakes_erros += 1
            error_msg = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_msg = f"[{e.response.status_code}] {e.response.text[:200]}"
            if DEBUG:
                print(f"   ❌ {fake.nome}: {error_msg}")
            error_acc_fakes.add_error(
                operation_type="criar_contato_fake",
                entity_id=fake.external_id_client,
                entity_name=fake.nome,
                error_message=f"{error_msg} | Payload: {payload}",
            )
finally:
    error_acc_fakes.save_errors()

print(f"✅ Fakes: {fakes_criados} criados | {fakes_erros} erros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Relatório Final

# COMMAND ----------

print("=" * 60)
print("📊 RELATÓRIO FINAL")
print("=" * 60)
print(f"   Donos:    {donos_criados} criados | {donos_erros} erros")
print(f"   Gerentes: {gerentes_criados} criados | {gerentes_erros} erros")
print(f"   Fakes:    {fakes_criados} criados | {fakes_erros} erros")
print(
    f"   Total:    {donos_criados + gerentes_criados + fakes_criados} criados | "
    f"{donos_erros + gerentes_erros + fakes_erros} erros"
)
print("=" * 60)
