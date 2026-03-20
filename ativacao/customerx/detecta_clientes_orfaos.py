# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Detecção de Clientes Órfãos no CustomerX
# MAGIC
# MAGIC Este notebook identifica clientes no CustomerX que violam o invariante de hierarquia:
# MAGIC
# MAGIC - **Matrizes órfãs**: redes que não possuem nenhuma filial associada no CustomerX
# MAGIC - **Filiais órfãs**: lojas que não possuem nenhuma matriz associada no CustomerX
# MAGIC
# MAGIC ## Estratégia
# MAGIC - **Somente leitura**: Não realiza nenhuma mutação no CustomerX
# MAGIC - **Auditoria**: Exibe no log os clientes órfãos encontrados
# MAGIC - **Alerta**: Falha o job se qualquer órfão for encontrado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from maggulake.customerx.notebook_setup import setup_customerx_notebook

# COMMAND ----------

env, customerx_client, _dry_run = setup_customerx_notebook(
    dbutils, "detecta_matrizes_orfas"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Buscar Todos os Clientes do CustomerX

# COMMAND ----------

# validacao_estrita=False pois pode haver clientes sem postgres_uuid (ex: criados manualmente)
existing_customers = customerx_client.fetch_all_customers(validacao_estrita=False)

matrizes = [c for c in existing_customers if c.eh_matriz]
filiais = [c for c in existing_customers if not c.eh_matriz]

print(
    f"📋 CustomerX: {len(existing_customers)} clientes total | "
    f"{len(matrizes)} matrizes | {len(filiais)} filiais"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Identificar Matrizes Órfãs
# MAGIC
# MAGIC Matrizes que não possuem nenhuma filial apontando para elas no CustomerX.

# COMMAND ----------

ids_matrizes_com_filiais = {
    filial.id_customerx_matriz
    for filial in filiais
    if filial.id_customerx_matriz is not None
}

matrizes_orfas = [
    matriz for matriz in matrizes if matriz.id_customerx not in ids_matrizes_com_filiais
]

print(f"🔍 Matrizes órfãs (sem filiais no CX): {len(matrizes_orfas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identificar Filiais Órfãs
# MAGIC
# MAGIC Filiais que não possuem nenhuma matriz associada no CustomerX.

# COMMAND ----------

filiais_orfas = [filial for filial in filiais if filial.id_customerx_matriz is None]

print(f"🔍 Filiais órfãs (sem matriz no CX): {len(filiais_orfas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exibir Auditoria no Log

# COMMAND ----------

if matrizes_orfas:
    print(f"\n⚠️  Matrizes órfãs ({len(matrizes_orfas)}):")
    for matriz in matrizes_orfas:
        print(
            f"   - [{matriz.id_customerx}] {matriz.nome_empresa} "
            f"(postgres_uuid={matriz.postgres_uuid})"
        )

if filiais_orfas:
    print(f"\n⚠️  Filiais órfãs ({len(filiais_orfas)}):")
    for filial in filiais_orfas:
        print(
            f"   - [{filial.id_customerx}] {filial.nome_empresa} "
            f"(postgres_uuid={filial.postgres_uuid})"
        )

if not matrizes_orfas and not filiais_orfas:
    print("✅ Nenhum cliente órfão encontrado — hierarquia CustomerX íntegra")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Falhar Job se Existirem Órfãos

# COMMAND ----------

erros: list[str] = []

if matrizes_orfas:
    erros.append(f"{len(matrizes_orfas)} matriz(es) sem filiais no CustomerX")

if filiais_orfas:
    erros.append(f"{len(filiais_orfas)} filial(is) sem matriz no CustomerX")

if erros:
    raise ValueError(f"❌ Clientes órfãos detectados no CustomerX: {'; '.join(erros)}.")

print("✅ Nenhum cliente órfão detectado!")
