# Databricks notebook source
# MAGIC %md
# MAGIC # Consolida Coluna `eh_medicamento` de Forma Consolidada
# MAGIC
# MAGIC Este notebook consolida a coluna `eh_medicamento` usando múltiplas fontes de dados em ordem de prioridade:
# MAGIC
# MAGIC 1. **Correções Manuais** (maior prioridade): Produtos corrigidos manualmente via expressões/listas
# MAGIC 2. **Categorias Cascata**: Produtos com categorias que começam com "Medicamentos"
# MAGIC 3. **Produtos Standard** (menor prioridade): Valor original da camada standard
# MAGIC
# MAGIC A tabela de saída (`coluna_eh_medicamento_completo`) inclui a fonte da determinação para auditoria.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment

# COMMAND ----------

from datetime import datetime, timezone

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import schema_coluna_eh_medicamento_completo

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "consolida_coluna_eh_medicamento",
    dbutils,
)

# COMMAND ----------

# Configurações
DEBUG = dbutils.widgets.get("debug") == "true"
spark = env.spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das Tabelas de Origem

# COMMAND ----------

# 1. Produtos Standard (base)
produtos_standard = env.table(Table.produtos_standard).select(
    "ean",
    "nome",
    F.col("eh_medicamento").alias("eh_medicamento_standard"),
)

if DEBUG:
    total_produtos = produtos_standard.count()
    print(f"📊 Total de produtos na camada standard: {total_produtos}")

# COMMAND ----------

# 2. Categorias Cascata (pega apenas o enriquecimento mais recente por EAN)
categorias_cascata = env.table(Table.categorias_cascata)

window_spec = Window.partitionBy("ean").orderBy(F.desc("gerado_em"))
categorias_cascata_recente = (
    categorias_cascata.withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select("ean", "categorias")
)

if DEBUG:
    total_categorias = categorias_cascata_recente.count()
    print(f"📊 Produtos com categorias cascata: {total_categorias}")

# COMMAND ----------

# 3. Correções Manuais

correcoes_manuais = env.table(Table.coluna_eh_medicamento_correcoes_manuais).select(
    "ean",
    F.col("eh_medicamento").alias("eh_medicamento_correcao"),
)

if DEBUG:
    total_correcoes = correcoes_manuais.count()
    print(f"📊 Produtos com correções manuais: {total_correcoes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento: Determina `eh_medicamento` por Fonte

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Analisa Categorias Cascata para Determinar se é Medicamento

# COMMAND ----------

# Explode das categorias e verifica se alguma começa com "Medicamentos"
categorias_exploded = categorias_cascata_recente.select(
    "ean", F.explode("categorias").alias("categoria")
)

# Identifica medicamentos: qualquer categoria que comece com "Medicamentos"
medicamentos_por_categoria = (
    categorias_exploded.filter(F.col("categoria.super_categoria").like("Medicamentos%"))
    .select("ean")
    .distinct()
    .withColumn("eh_medicamento_categoria", F.lit(True))
)

# Identifica não-medicamentos: categorias que NÃO são "Medicamentos"
nao_medicamentos_por_categoria = (
    categorias_exploded.filter(
        ~F.col("categoria.super_categoria").like("Medicamentos%")
    )
    .select("ean")
    .distinct()
    .withColumn("eh_medicamento_categoria", F.lit(False))
)

# Combina: prioriza medicamentos (se tiver pelo menos uma categoria de medicamento, é medicamento)
eh_medicamento_por_categoria = (
    medicamentos_por_categoria.unionByName(nao_medicamentos_por_categoria)
    .groupBy("ean")
    .agg(F.max("eh_medicamento_categoria").alias("eh_medicamento_categoria"))
)

if DEBUG:
    meds = eh_medicamento_por_categoria.filter(
        F.col("eh_medicamento_categoria") == True
    ).count()
    nao_meds = eh_medicamento_por_categoria.filter(
        F.col("eh_medicamento_categoria") == False
    ).count()
    print("📊 Por categorias cascata:")
    print(f"   - Medicamentos: {meds}")
    print(f"   - Não medicamentos: {nao_meds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Aplica Prioridade das Fontes

# COMMAND ----------

# Join de todas as fontes
resultado = produtos_standard.join(
    eh_medicamento_por_categoria, on="ean", how="left"
).join(correcoes_manuais, on=["ean"], how="left")

# Aplica prioridade usando COALESCE:
# 1. Correção manual (maior prioridade)
# 2. Categoria cascata
# 3. Standard (menor prioridade)
resultado = resultado.withColumn(
    "eh_medicamento",
    F.coalesce(
        F.col("eh_medicamento_correcao"),
        F.col("eh_medicamento_categoria"),
        F.col("eh_medicamento_standard"),
    ),
)

# Determina a fonte
resultado = resultado.withColumn(
    "fonte",
    F.when(F.col("eh_medicamento_correcao").isNotNull(), F.lit("correcao_manual"))
    .when(F.col("eh_medicamento_categoria").isNotNull(), F.lit("categoria_cascata"))
    .when(F.col("eh_medicamento_standard").isNotNull(), F.lit("produtos_standard"))
    .otherwise(F.lit("nao_determinado")),
)

# Adiciona timestamp
resultado = resultado.withColumn("atualizado_em", F.lit(datetime.now(timezone.utc)))

# Seleciona apenas as colunas necessárias
resultado_final = resultado.select("ean", "eh_medicamento", "fonte", "atualizado_em")

# Remove duplicatas por EAN (caso existam)
# Mantém o registro mais recente ou o primeiro encontrado
resultado_final = resultado_final.dropDuplicates(["ean"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report: Estatísticas de Preenchimento

# COMMAND ----------

if DEBUG:
    print("\n" + "=" * 70)
    print("📊 REPORT: Estatísticas da Coluna eh_medicamento")
    print("=" * 70)

    total = resultado_final.count()

    # Distribuição por fonte
    print("\nDistribuição por Fonte:")
    distribuicao_fonte = (
        resultado_final.groupBy("fonte").count().orderBy(F.desc("count"))
    )
    for row in distribuicao_fonte.collect():
        porcentagem = (row['count'] / total) * 100
        print(f"   - {row['fonte']}: {row['count']} ({porcentagem:.2f}%)")

    # Distribuição por valor
    print("\nDistribuição por Valor:")
    medicamentos = resultado_final.filter(F.col("eh_medicamento") == True).count()
    nao_medicamentos = resultado_final.filter(F.col("eh_medicamento") == False).count()
    nulos = resultado_final.filter(F.col("eh_medicamento").isNull()).count()

    print(
        f"   - Medicamentos (True): {medicamentos} ({(medicamentos / total) * 100:.2f}%)"
    )
    print(
        f"   - Não Medicamentos (False): {nao_medicamentos} ({(nao_medicamentos / total) * 100:.2f}%)"
    )
    print(f"   - Não Determinados (NULL): {nulos} ({(nulos / total) * 100:.2f}%)")

    # Taxa de preenchimento
    print("\nTaxa de Preenchimento:")
    preenchidos = resultado_final.filter(F.col("eh_medicamento").isNotNull()).count()
    taxa_preenchimento = (preenchidos / total) * 100
    print(f"   - Preenchidos: {preenchidos} ({taxa_preenchimento:.2f}%)")
    print(f"   - Vazios: {nulos} ({(nulos / total) * 100:.2f}%)")

    # Distribuição cruzada: fonte x valor
    print("\nDistribuição Cruzada (Fonte x Valor):")
    distribuicao_cruzada = (
        resultado_final.groupBy("fonte", "eh_medicamento")
        .count()
        .orderBy("fonte", "eh_medicamento")
    )
    for row in distribuicao_cruzada.collect():
        print(
            f"   - {row['fonte']} | eh_medicamento={row['eh_medicamento']}: {row['count']}"
        )

    print("\n" + "=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização de Exemplos

# COMMAND ----------

if DEBUG:
    print("\n📋 Exemplos de produtos por fonte:\n")

    # Exemplos de cada fonte
    for fonte in [
        "correcao_manual",
        "categoria_cascata",
        "produtos_standard",
        "nao_determinado",
    ]:
        exemplos = (
            resultado.filter(F.col("fonte") == fonte)
            .select("ean", "nome", "eh_medicamento", "fonte")
            .limit(5)
        )

        count = resultado.filter(F.col("fonte") == fonte).count()
        if count > 0:
            print(f"\n🔹 Fonte: {fonte} ({count} produtos)")
            exemplos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar na Tabela de Enriquecimento

# COMMAND ----------

# Cria a tabela se não existir
env.create_table_if_not_exists(
    Table.coluna_eh_medicamento_completo,
    schema_coluna_eh_medicamento_completo,
)

print(f"✅ Tabela {Table.coluna_eh_medicamento_completo.value} criada/verificada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação de Duplicatas

# COMMAND ----------

# Verifica se há duplicatas por EAN antes do MERGE
duplicatas = resultado_final.groupBy("ean").count().filter(F.col("count") > 1)

num_duplicatas = duplicatas.count()

if num_duplicatas > 0:
    print(f"⚠️ ATENÇÃO: Encontradas {num_duplicatas} EANs duplicados no resultado_final")
    if DEBUG:
        print("\n📋 Exemplos de EANs duplicados:")
        duplicatas.limit(10).display()

    raise ValueError(
        f"Erro: {num_duplicatas} EANs duplicados encontrados em resultado_final. "
        "Isso causará erro no MERGE. Verifique a lógica de JOIN e deduplicação."
    )
else:
    print(
        f"✅ Validação OK: Nenhum ID duplicado encontrado (total: {resultado_final.count()} registros únicos)"
    )

# COMMAND ----------

# Salva usando MERGE (upsert)
delta_table = DeltaTable.forName(spark, Table.coluna_eh_medicamento_completo.value)

delta_table.alias("target").merge(
    resultado_final.alias("source"), "target.ean = source.ean"
).whenMatchedUpdate(
    set={
        "eh_medicamento": "source.eh_medicamento",
        "fonte": "source.fonte",
        "atualizado_em": "source.atualizado_em",
    }
).whenNotMatchedInsert(
    values={
        "ean": "source.ean",
        "eh_medicamento": "source.eh_medicamento",
        "fonte": "source.fonte",
        "atualizado_em": "source.atualizado_em",
    }
).execute()

print(f"✅ Dados salvos na tabela {Table.coluna_eh_medicamento_completo.value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação Final

# COMMAND ----------

if DEBUG:
    df_validacao = env.table(Table.coluna_eh_medicamento_completo)

    print(f"\n✅ Validação da tabela {Table.coluna_eh_medicamento_completo.value}:")
    print(f"   - Total de registros: {df_validacao.count()}")

    print("\n📋 Amostra dos dados salvos:")
    df_validacao.orderBy(F.desc("atualizado_em")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos Passos
# MAGIC
# MAGIC Esta tabela (`coluna_eh_medicamento_completo`) pode ser usada em:
# MAGIC
# MAGIC 1. **`agrega_enriquecimentos.py`**: Para atualizar a coluna `eh_medicamento` na camada refined
# MAGIC 2. **Análises**: Para entender a distribuição de medicamentos vs não-medicamentos
# MAGIC 3. **Auditoria**: Campo `fonte` permite rastrear a origem de cada determinação
# MAGIC 4. **Refinamento**: Identificar produtos "nao_determinado" que precisam de atenção manual
