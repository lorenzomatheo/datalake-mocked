# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Associação de Filiais às Matrizes no CustomerX
# MAGIC
# MAGIC Este notebook vincula filiais (lojas) às suas matrizes (redes) no CustomerX.
# MAGIC
# MAGIC Obs:
# MAGIC - Execute `cria_matrizes_filiais.py` antes deste.
# MAGIC - NOTE: Apenas associa - não cria nem atualiza clientes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from collections import defaultdict

import requests

from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import ErrorAccumulator
from maggulake.integrations.customerx.models.conta import ContaDTO
from maggulake.integrations.customerx.models.loja import LojaDTO

# COMMAND ----------

dbutils.widgets.dropdown("validacao_estrita", "true", ["true", "false"])

# COMMAND ----------

env, customerx_client, dry_run = setup_customerx_notebook(
    dbutils, "associa_matrizes_filiais"
)

VALIDACAO_ESTRITA = dbutils.widgets.get("validacao_estrita").lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extração de Dados do Postgres

# COMMAND ----------

lojas_df = env.postgres_replica_adapter.get_lojas(
    env.spark,
    ativo=None,
    extra_fields=[
        "status",
        "created_at",
    ],
)
lojas_raw = [row.asDict() for row in lojas_df.collect()]
lojas_postgres = [LojaDTO.from_postgres_row(row) for row in lojas_raw]
loja_para_conta_postgres = {
    loja.postgres_uuid: loja.conta_postgres_uuid for loja in lojas_postgres
}

# COMMAND ----------

contas_df = env.postgres_replica_adapter.get_contas(env.spark)
contas_raw = [row.asDict() for row in contas_df.collect()]
contas_postgres = [ContaDTO.from_postgres_row(row) for row in contas_raw]
conta_names = {conta.postgres_uuid: conta.nome for conta in contas_postgres}

# COMMAND ----------

n_contas_postgres = len(contas_postgres)
n_lojas_postgres = len(lojas_postgres)

print(
    f"Lojas: {n_lojas_postgres} | "
    f"Contas: {n_contas_postgres} | "
    f"Total: {n_lojas_postgres + n_contas_postgres}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Buscar Clientes Existentes no CustomerX

# COMMAND ----------

existing_customers = customerx_client.fetch_all_customers(
    validacao_estrita=VALIDACAO_ESTRITA,
)

# COMMAND ----------

uuids_matrizes_existentes = {
    c.postgres_uuid for c in existing_customers if c.postgres_uuid and c.eh_matriz
}

uuids_filiais_existentes = {
    c.postgres_uuid for c in existing_customers if c.postgres_uuid and not c.eh_matriz
}

associacoes_existentes = {
    c.postgres_uuid: c.id_customerx_matriz
    for c in existing_customers
    if c.postgres_uuid and not c.eh_matriz
}

# COMMAND ----------

total_associacoes_existentes = len([v for v in associacoes_existentes.values() if v])

print(
    f"CustomerX: {len(existing_customers)} clientes | "
    f" {len(uuids_matrizes_existentes)} matrizes | "
    f"{len(uuids_filiais_existentes)} filiais"
)
print(f"Quantidade de associações existentes: {total_associacoes_existentes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agrupar Filiais por Matriz

# COMMAND ----------

lojas_por_matriz: dict[str, list[str]] = defaultdict(list)
matrizes_com_filiais_pendentes: set[str] = set()
filiais_ja_associadas_corretamente = 0
filiais_associadas_matriz_errada = 0
filiais_novas = 0

for loja_uuid, conta_uuid in loja_para_conta_postgres.items():
    if (
        loja_uuid not in uuids_filiais_existentes
        or conta_uuid not in uuids_matrizes_existentes
    ):
        # Nao tem como associar se a filial ou matriz nao existem no CX
        continue

    # Acha o id_customerx da matriz
    matriz_id_customerx = next(
        (
            c.id_customerx
            for c in existing_customers
            if c.postgres_uuid == conta_uuid and c.eh_matriz
        ),
        None,
    )

    associacao_atual = associacoes_existentes.get(loja_uuid)

    if associacao_atual == matriz_id_customerx:
        filiais_ja_associadas_corretamente += 1
    else:
        # Filial nova ou associada à matriz errada — precisamos atualizar esta matriz
        if associacao_atual is not None:
            filiais_associadas_matriz_errada += 1
        else:
            filiais_novas += 1
        matrizes_com_filiais_pendentes.add(conta_uuid)

    # IMPORTANTE: Sempre adiciona a filial na lista da sua matriz.
    # O PUT /branch_clients é um REPLACE — precisamos enviar a lista COMPLETA
    # de filiais, não apenas as novas, senão as já associadas seriam removidas.
    lojas_por_matriz[conta_uuid].append(loja_uuid)

# Só precisa chamar a API para matrizes que têm pelo menos 1 filial nova/errada
lojas_por_matriz = {
    k: v for k, v in lojas_por_matriz.items() if k in matrizes_com_filiais_pendentes
}

total_filiais_no_payload = sum(len(lojas) for lojas in lojas_por_matriz.values())
matrizes_a_atualizar = len(lojas_por_matriz)

print("📊 Resumo:")
print(f"\tFiliais já corretas (sem mudança): {filiais_ja_associadas_corretamente}")
print(f"\tFiliais novas a associar: {filiais_novas}")
print(f"\tFiliais a re-associar (matriz errada): {filiais_associadas_matriz_errada}")
print(f"\tMatrizes a atualizar: {matrizes_a_atualizar}")
print(f"\tTotal filiais no payload (inclui existentes): {total_filiais_no_payload}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Associar Filiais às Matrizes

# COMMAND ----------

associacoes_sucesso = 0
associacoes_falhadas = 0
filiais_associadas_total = 0
error_acc = ErrorAccumulator(spark, env.settings.catalog, "associa_matrizes_filiais")

print(f"🔗 Associando filiais ({matrizes_a_atualizar} matrizes)...")

try:
    for matriz_uuid, filiais_uuids_list in lojas_por_matriz.items():
        if not filiais_uuids_list:
            continue  # Matriz nao tem nenhuma filial

        if not matriz_uuid:
            continue  # Codigo da matriz nao eh valido

        if dry_run:
            associacoes_sucesso += 1
            filiais_associadas_total += len(filiais_uuids_list)
            continue

        try:
            customerx_client.associar_filiais(
                id_curto_cx_matriz=matriz_uuid,
                ids_curtos_cx_filiais=filiais_uuids_list,
            )
            associacoes_sucesso += 1
            filiais_associadas_total += len(filiais_uuids_list)
        except requests.exceptions.RequestException as e:
            associacoes_falhadas += 1
            error_acc.add_error(
                operation_type="associar_filiais",
                entity_id=matriz_uuid,
                entity_name=conta_names.get(matriz_uuid),
                error_message=str(e),
            )
finally:
    error_acc.save_errors()

print(f"✅ Matrizes: {associacoes_sucesso} sucesso | {associacoes_falhadas} falhadas")
print(f"✅ Filiais associadas: {filiais_associadas_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Relatório Final

# COMMAND ----------

filiais_nao_encontradas = [
    loja_uuid
    for loja_uuid in loja_para_conta_postgres
    if loja_uuid not in uuids_filiais_existentes
]

matrizes_nao_encontradas = {
    conta_uuid
    for conta_uuid in set(loja_para_conta_postgres.values())
    if conta_uuid not in uuids_matrizes_existentes
}

if filiais_nao_encontradas or matrizes_nao_encontradas:
    print(
        f"Filiais not found in CustomerX (exist in Postgres): {len(filiais_nao_encontradas)}"
    )
    print(
        f"Matrizes not found in CustomerX (exist in Postgres): {len(matrizes_nao_encontradas)}"
    )
    print("Run cria_matrizes_filiais.py to create missing entities in CustomerX")
