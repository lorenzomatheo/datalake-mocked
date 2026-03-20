# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Criação de Grupos no CustomerX
# MAGIC
# MAGIC Este notebook cria grupos no CustomerX a partir das contas (redes) no Postgres.
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Diff-based sync**: Busca todos os grupos do CustomerX e compara com Postgres
# MAGIC - **Create-only**: Não atualiza registros existentes, apenas cria novos
# MAGIC - **Idempotente**: Usa `external_id` (UUID da conta do Postgres) para evitar duplicação
# MAGIC - **Um grupo por rede**: Cada rede/conta vira um grupo para facilitar filtros no CX
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração e Inicialização

# COMMAND ----------

import requests

from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import ErrorAccumulator
from maggulake.integrations.customerx.models.group import GroupDTO

# COMMAND ----------

env, customerx_client, dry_run = setup_customerx_notebook(dbutils, "cria_grupos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extração de Contas do Postgres

# COMMAND ----------

# Adapter já filtra contas de teste/obsoletas automaticamente
contas_df = env.postgres_replica_adapter.get_contas(spark)
contas_raw = [row.asDict() for row in contas_df.collect()]

grupos_to_sync = [
    GroupDTO(
        external_id=str(conta["id"]),
        description=conta["name"],
        status=True,
    )
    for conta in contas_raw
]

print(f"📊 Contas no Postgres: {len(grupos_to_sync)} a sincronizar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Comparar com Grupos Existentes

# COMMAND ----------

existing_groups = customerx_client.fetch_all_groups()
existing_group_ids = {group.external_id for group in existing_groups}

grupos_to_create = [
    grupo for grupo in grupos_to_sync if grupo.external_id not in existing_group_ids
]

print(
    f"📊 Grupos no Postgres: {len(grupos_to_sync)} | "
    f"Já no CX: {len(grupos_to_sync) - len(grupos_to_create)} | "
    f"A criar: {len(grupos_to_create)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Grupos no CustomerX

# COMMAND ----------

if not grupos_to_create:
    print("✅ Nenhum grupo novo a criar - todos já existem no CustomerX!")
    dbutils.notebook.exit("success")

# COMMAND ----------

grupos_criados = 0
grupos_falhados = 0
grupos_com_erro = []
error_acc = ErrorAccumulator(spark, env.settings.catalog, "cria_grupos")

print(f"🏢 Criando {len(grupos_to_create)} grupos...")

try:
    for grupo in grupos_to_create:
        if dry_run:
            grupos_criados += 1
            continue

        payload = grupo.to_customerx_payload()

        try:
            customerx_client.criar_grupo(payload)
            print(f"\t\t✅ Grupo criado: {grupo.description}")
            grupos_criados += 1
        except requests.RequestException as e:
            grupos_falhados += 1
            grupos_com_erro.append((grupo.description, grupo.external_id, str(e)))
            error_acc.add_error(
                operation_type="criar_grupo",
                entity_id=grupo.external_id,
                entity_name=grupo.description,
                error_message=str(e),
            )
            print(f"\t\t❌ Erro ao criar grupo '{grupo.description}': {str(e)}")
finally:
    error_acc.save_errors()

print(f"\n✅ Concluído: {grupos_criados} criados, {grupos_falhados} erros")

# COMMAND ----------

if grupos_com_erro:
    print("\n\tGrupos com erro:")
    for nome, uuid, erro in grupos_com_erro:
        print(f"   - {nome} ({uuid}): {erro}")
