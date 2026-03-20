# Databricks notebook source
# MAGIC %md
# MAGIC # Reset de Ambiente Sandbox - CustomerX
# MAGIC
# MAGIC Este notebook realiza limpeza completa do ambiente sandbox CustomerX,
# MAGIC removendo TODOS os contatos, clientes e grupos.
# MAGIC Obs.:
# MAGIC 1. **Este notebook é DESTRUTIVO e IRREVERSÍVEL**
# MAGIC 2. **Se preferir, pode entrar em contato com o time do CX pedindo para eles resetarem o sandbox para você**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.integrations.customerx.reset import CustomerXSandboxReset

# COMMAND ----------

env, customerx_client, dry_run = setup_customerx_notebook(dbutils, "reset_customerx")

# NOTE: hardcode for safety
customerx_client.cx_environment = "sandbox"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar Estado Atual do Sandbox

# COMMAND ----------

reset_manager = CustomerXSandboxReset(
    client=customerx_client,
    environment=customerx_client.cx_environment,
    request_interval_seconds=1,
)

# NOTE: A validacao_estrita eh ignorada pois queremos resetar tudo, mesmo clientes sem postgres_uuid
clientes = customerx_client.fetch_all_customers(validacao_estrita=False)
contacts = customerx_client.fetch_all_contacts()
groups = customerx_client.fetch_all_groups()

print(
    f"📊 Sandbox: {len(clientes)} clientes | {len(contacts)} contatos | {len(groups)} grupos"
)

if len(clientes) == 0 and len(contacts) == 0 and len(groups) == 0:
    print("✅ Sandbox já está limpo!")
    dbutils.notebook.exit("Sandbox já está limpo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar Reset (DESTRUTIVO!)

# COMMAND ----------

print("🔥 INICIANDO RESET DO SANDBOX...")
result = reset_manager.reset_environment(clientes_existentes=clientes, debug=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificação Pós-Reset

# COMMAND ----------


clientes_restantes = customerx_client.fetch_all_customers(validacao_estrita=False)
contacts_restantes = customerx_client.fetch_all_contacts()
groups_restantes = customerx_client.fetch_all_groups()

total_restante = (
    len(clientes_restantes) + len(contacts_restantes) + len(groups_restantes)
)
if total_restante == 0:
    print("✅ SUCESSO: Sandbox completamente limpo!")
else:
    print(f"⚠️  {total_restante} entidades ainda existem no sandbox")
