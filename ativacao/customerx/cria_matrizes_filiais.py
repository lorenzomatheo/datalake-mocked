# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Criação de Matrizes e Filiais no CustomerX
# MAGIC
# MAGIC Este notebook realiza a criação de clientes no CustomerX a partir do env.postgres_replica_adapter.
# MAGIC ## Estratégia
# MAGIC - **Diff-based sync**: Busca todos os clientes do CustomerX e compara com Postgres
# MAGIC - **Create-only**: Não atualiza registros existentes
# MAGIC - **Idempotente**: Usa `external_id` (UUID do Postgres) para evitar duplicação
# MAGIC - **Two-phase**: Cria matrizes (redes) primeiro, depois filiais (lojas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from collections import Counter, defaultdict

import requests

from maggulake.ativacao.customerx.notebook_setup import setup_customerx_notebook
from maggulake.ativacao.customerx.reporting import ErrorAccumulator
from maggulake.integrations.customerx.models.conta import ContaDTO
from maggulake.integrations.customerx.models.loja import LojaDTO

# COMMAND ----------

dbutils.widgets.dropdown("validacao_estrita", "true", ["true", "false"])

# COMMAND ----------

env, customerx_client, dry_run = setup_customerx_notebook(
    dbutils, "cria_matrizes_filiais"
)

# NOTE: se desligado, evita os erros de "ERRO CRÍTICO: Cliente CustomerX não possui 'postgres_uuid' nos custom_attributes!
VALIDACAO_ESTRITA = dbutils.widgets.get("validacao_estrita").lower() == "true"

DEBUG = dbutils.widgets.get("debug").lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extração de Dados do Postgres

# COMMAND ----------

contas_df = env.postgres_replica_adapter.get_contas(spark)

if DEBUG:
    contas_df.limit(5).display()

# COMMAND ----------

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
        "created_at",
        "updated_at",
    ],
)

if DEBUG:
    lojas_df.limit(5).display()

# COMMAND ----------
contas_raw = [row.asDict() for row in contas_df.collect()]
lojas_raw = [row.asDict() for row in lojas_df.collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Calcular Estado Mais Frequente por Conta

# COMMAND ----------

lojas_por_conta_estado = defaultdict(list)
for loja in lojas_raw:
    if loja.get('estado'):
        lojas_por_conta_estado[str(loja['conta_id'])].append(loja['estado'])

contagem_estados = {
    conta_id: Counter(estados).most_common(1)[0][0]
    for conta_id, estados in lojas_por_conta_estado.items()
    if estados
}

# COMMAND ----------

contas_postgres = [
    ContaDTO.from_postgres_row(row, estado=contagem_estados.get(str(row['id'])))
    for row in contas_raw
]
lojas_postgres = [LojaDTO.from_postgres_row(row) for row in lojas_raw]
conta_names = {conta.postgres_uuid: conta.nome for conta in contas_postgres}

print("Dados no postgres:")
print(f"📊 Contas: {len(contas_postgres)} | Lojas: {len(lojas_postgres)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Filtrar Lojas por Contas Válidas

# COMMAND ----------

# Garante que todas as lojas estão associadas a contas válidas
# Isso é um double check apenas, não quero lojas órfãs no CustomerX
contas_validas_uuids = {conta.postgres_uuid for conta in contas_postgres}
qtde_lojas_antes_filtro = len(lojas_postgres)
lojas_postgres = [
    loja for loja in lojas_postgres if loja.conta_postgres_uuid in contas_validas_uuids
]

print(
    f"🔍 Lojas filtradas: {qtde_lojas_antes_filtro - len(lojas_postgres)} lojas sem conta válida removidas"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Buscar Clientes e Grupos Existentes no CustomerX

# COMMAND ----------

existing_customers = customerx_client.fetch_all_customers(
    validacao_estrita=VALIDACAO_ESTRITA, customers_per_page=20
)

# COMMAND ----------

# Buscar grupos para mapear conta UUID -> group ID
# Grupos são usados para popular external_id_group no payload, permitindo
# vincular customers (matrizes/filiais) aos seus grupos (redes) no CustomerX
existing_groups = customerx_client.fetch_all_groups()

# COMMAND ----------

grupos_por_conta = {group.external_id: group.external_id for group in existing_groups}
customers_com_uuid = [c for c in existing_customers if c.postgres_uuid]
customers_sem_uuid = [c for c in existing_customers if not c.postgres_uuid]
existing_uuids = {c.postgres_uuid for c in customers_com_uuid}

print(
    f"📋 CustomerX: {len(existing_customers)} clientes | {len(grupos_por_conta)} grupos"
)
if customers_sem_uuid:
    print(
        f"⚠️  {len(customers_sem_uuid)} clientes sem postgres_uuid (provavelmente foram criados manualmente)"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Filtrar Contas sem Lojas (Prevenir Matrizes Órfãs)

# COMMAND ----------

contas_com_lojas = set(loja.conta_postgres_uuid for loja in lojas_postgres)
contas_antes_filtro_lojas = len(contas_postgres)
contas_postgres = [
    conta for conta in contas_postgres if conta.postgres_uuid in contas_com_lojas
]

print(
    f"🔍 Removidas {contas_antes_filtro_lojas - len(contas_postgres)} contas sem lojas"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calcular Diff

# COMMAND ----------

contas_to_create = [c for c in contas_postgres if c.postgres_uuid not in existing_uuids]
lojas_to_create = [
    loja for loja in lojas_postgres if loja.postgres_uuid not in existing_uuids
]

lojas_to_create_por_conta = Counter(
    loja.conta_postgres_uuid for loja in lojas_to_create
)

print("📊 Resumo:")
print(f"\tContas: {len(contas_postgres)} total | {len(contas_to_create)} a criar")
print(f"\tLojas: {len(lojas_postgres)} total | {len(lojas_to_create)} a criar")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fase 1: Criar Matrizes (Redes)

# COMMAND ----------

redes_criadas = 0
redes_falhadas = 0
redes_com_erro = []
error_acc = ErrorAccumulator(spark, env.settings.catalog, "cria_matrizes_filiais")

print(f"🏢 Criando matrizes ({len(contas_to_create)})...")


try:
    for conta in contas_to_create:
        if dry_run:
            redes_criadas += 1
            continue

        group_id = grupos_por_conta.get(conta.postgres_uuid)
        payload = conta.to_customerx_payload(external_id_group=group_id)

        try:
            result = customerx_client.criar_cliente_com_payload(payload)

            if result.get("id"):
                redes_criadas += 1
                print(f"✅ Matriz criada: {conta.nome} ({conta.postgres_uuid})")
            else:
                redes_falhadas += 1

        except requests.RequestException as e:
            redes_falhadas += 1
            redes_com_erro.append((conta.nome, conta.postgres_uuid, str(e)))
            error_acc.add_error(
                operation_type="criar_matriz",
                entity_id=conta.postgres_uuid,
                entity_name=conta.nome,
                error_message=str(e),
            )
finally:
    pass  # Não salva ainda pois temos mais um loop abaixo

print(f"✅ Matrizes: {redes_criadas} criadas | {redes_falhadas} falhadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.1. Validação: Remover Lojas Órfãs

# COMMAND ----------

if redes_com_erro and not dry_run:
    matrizes_falhadas_uuids = {uuid for _, uuid, _ in redes_com_erro}
    lojas_orfas_count = len(
        [
            loja
            for loja in lojas_to_create
            if loja.conta_postgres_uuid in matrizes_falhadas_uuids
        ]
    )
    lojas_to_create = [
        loja
        for loja in lojas_to_create
        if loja.conta_postgres_uuid not in matrizes_falhadas_uuids
    ]
    print(f"⚠️  Removidas lojas órfãs (matrizes falharam): {lojas_orfas_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fase 2: Criar Filiais (Lojas)

# COMMAND ----------

lojas_criadas = 0
lojas_falhadas = 0
lojas_com_erro: list[tuple[str, str, str, str]] = []  # (nome, uuid, conta_name, error)

print(f"🏪 Criando filiais ({len(lojas_to_create)})...")

try:
    for loja in lojas_to_create:
        conta_name = conta_names.get(loja.conta_postgres_uuid)
        if conta_name is None:
            raise ValueError(
                f"❌ Loja '{loja.nome}' referencia uma conta inexistente: {loja.conta_postgres_uuid}"
            )

        if dry_run:
            lojas_criadas += 1
            continue

        group_id = grupos_por_conta.get(loja.conta_postgres_uuid)
        payload = loja.to_customerx_payload(external_id_group=group_id)

        try:
            result = customerx_client.criar_cliente_com_payload(payload)

            if result.get("id"):
                lojas_criadas += 1
                print(f"✅ Filial criada: {loja.nome} ({loja.postgres_uuid})")
            else:
                lojas_falhadas += 1

        except requests.RequestException as e:
            lojas_falhadas += 1
            lojas_com_erro.append((loja.nome, loja.postgres_uuid, conta_name, str(e)))
            error_acc.add_error(
                operation_type="criar_filial",
                entity_id=loja.postgres_uuid,
                entity_name=loja.nome,
                error_message=str(e),
            )
finally:
    # Salva todos os erros acumulados (matrizes + filiais)
    error_acc.save_errors()

print(f"✅ Filiais: {lojas_criadas} criadas | {lojas_falhadas} falhadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Relatório Final

# COMMAND ----------

if redes_com_erro or lojas_com_erro:
    print("❌ Erros detectados:")
    if redes_com_erro:
        print(f"   Matrizes: {len(redes_com_erro)}")
    if lojas_com_erro:
        print(f"   Filiais: {len(lojas_com_erro)}")
else:
    print("✅ Nenhum erro encontrado!")
