# Databricks notebook source
# MAGIC %md
# MAGIC # Agrega Categorias
# MAGIC
# MAGIC Este notebook consolida as categorias geradas pelo processo de categorização em cascata
# MAGIC e as integra com a tabela de produtos standard, atualizando o campo `eh_medicamento`
# MAGIC baseado nas categorias identificadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql.window import Window

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.schemas import agrega_categorias
from maggulake.utils.time import agora_em_sao_paulo, agora_em_sao_paulo_str

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "agrega_categorias",
    dbutils,
)

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug").lower() == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê tabela de produtos standard

# COMMAND ----------

produtos_standard = env.table(Table.produtos_standard).cache()

# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Total de produtos na tabela standard: {produtos_standard.count()}"
    )
    produtos_standard.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lê categorias geradas pelo código de categorização em cascata
# MAGIC
# MAGIC Para cada EAN, pegamos apenas o enriquecimento mais recente (coluna "gerado_em" mais recente).

# COMMAND ----------

categorias_cascata = env.table(Table.categorias_cascata)

# COMMAND ----------

if DEBUG:
    total_enriquecimentos = categorias_cascata.count()
    eans_unicos = categorias_cascata.select("ean").distinct().count()
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Enriquecimentos existentes: {total_enriquecimentos} registros"
    )
    print(f"[{agora_em_sao_paulo_str()}] 📊 EANs únicos categorizados: {eans_unicos}")

# COMMAND ----------

# Seleciona apenas o enriquecimento mais recente por EAN
window_spec = Window.partitionBy("ean").orderBy(F.desc("gerado_em"))
categorias_cascata_recente = (
    categorias_cascata.withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Enriquecimentos mais recentes por EAN: {categorias_cascata_recente.count()}"
    )
    categorias_cascata_recente.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transforma estrutura de categorias em formato flat
# MAGIC
# MAGIC Converte o array de structs em array de strings no formato: "Super Categoria -> Meso Categoria -> Micro Categoria"

# COMMAND ----------

# Transforma array de structs em array de strings flat
categorias_flat = (
    categorias_cascata_recente.withColumn(
        "categorias_flat",
        F.transform(
            F.col("categorias"),
            lambda cat: F.when(
                (cat["micro_categoria"].isNotNull())
                & (cat["micro_categoria"] != "null")
                & (F.trim(cat["micro_categoria"]) != ""),
                F.concat_ws(
                    " -> ",
                    cat["super_categoria"],
                    cat["meso_categoria"],
                    cat["micro_categoria"],
                ),
            ).otherwise(
                F.concat_ws(
                    " -> ",
                    cat["super_categoria"],
                    cat["meso_categoria"],
                )
            ),
        ),
    )
    .withColumn(
        "super_categorias",
        F.transform(F.col("categorias"), lambda cat: cat["super_categoria"]),
    )
    .select("ean", "gerado_em", "categorias_flat", "super_categorias")
)

# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Quantidade de Categorias: {categorias_flat.count()}"
    )
    categorias_flat.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-definindo coluna `eh_medicamento`
# MAGIC
# MAGIC Lógica:
# MAGIC - Se qualquer super_categoria for "Medicamentos", então `eh_medicamento` = True
# MAGIC - Senão, `eh_medicamento` = False
# MAGIC - Se não houver categoria, mantém o valor original da tabela standard

# COMMAND ----------

# Determina eh_medicamento baseado nas categorias
# Se houver múltiplas categorias, usamos OR lógico:
# se qualquer uma for "Medicamentos", então eh_medicamento = True
categorias_agregadas_por_ean = categorias_flat.withColumn(
    "eh_medicamento_categoria",
    F.when(
        F.array_contains(F.col("super_categorias"), "Medicamentos"), F.lit(True)
    ).otherwise(F.lit(False)),
).select("ean", "categorias_flat", "eh_medicamento_categoria", "gerado_em")

# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Categorias agregadas por EAN: {categorias_agregadas_por_ean.count()}"
    )
    categorias_agregadas_por_ean.limit(20).display()

# COMMAND ----------


# Join com produtos standard e define eh_medicamento final
produtos_final = (
    produtos_standard.select("ean", "eh_medicamento")
    .join(categorias_agregadas_por_ean, on="ean", how="left")
    .withColumn(
        "eh_medicamento",
        F.coalesce(F.col("eh_medicamento_categoria"), F.col("eh_medicamento")),
    )
    .withColumn(
        "atualizado_em",
        F.coalesce(
            F.col("gerado_em"),
            F.lit(agora_em_sao_paulo()),
        ),
    )
    .select("ean", "categorias_flat", "eh_medicamento", "atualizado_em")
    .withColumnRenamed("categorias_flat", "categorias")
)

# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Produtos com categorias finais: {produtos_final.count()}"
    )
    produtos_final.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepara dados para salvar
# MAGIC
# MAGIC Mantém as categorias como array para preservar múltiplas categorias por produto.

# COMMAND ----------

# Cada EAN tem um array de categorias
produtos_final_preparado = produtos_final.select(
    "ean", "categorias", "eh_medicamento", "atualizado_em"
)

# COMMAND ----------

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 📊 Tamanho df final: {produtos_final_preparado.count()}"
    )
    produtos_final_preparado.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação: Alterações em `eh_medicamento`
# MAGIC
# MAGIC Mostra quantos produtos tiveram o valor de `eh_medicamento` alterado entre standard e refined.
# MAGIC TODO: Essa parte de prints e comparacao seria melhor deixar num notebook separado, pra acelerar o pipeline

# COMMAND ----------

# Compara eh_medicamento entre standard e refined
comparacao = produtos_standard.select(
    "ean",
    "nome",
    F.col("eh_medicamento").alias("eh_medicamento_standard"),
).join(
    produtos_final_preparado.select("ean", "eh_medicamento")
    .distinct()
    .withColumnRenamed("eh_medicamento", "eh_medicamento_refined"),
    on=["ean"],
    how="inner",
)

# COMMAND ----------

# Produtos que mudaram de valor e estatísticas
produtos_que_mudaram = comparacao.filter(
    F.col("eh_medicamento_standard") != F.col("eh_medicamento_refined")
)

total_produtos_comparacao = comparacao.count()
total_mudancas = produtos_que_mudaram.count()
percentual_mudanca = (
    (total_mudancas / total_produtos_comparacao * 100)
    if total_produtos_comparacao > 0
    else 0
)

print(f"\n{'=' * 80}")
print("📊 COMPARAÇÃO: Alterações em `eh_medicamento` (Standard → Refined)")
print(f"{'=' * 80}")
print(f"\nTotal de produtos comparados: {total_produtos_comparacao}")
print(f"Produtos com alteração: {total_mudancas} ({percentual_mudanca:.2f}%)")
print(f"Produtos sem alteração: {total_produtos_comparacao - total_mudancas}")

# COMMAND ----------

# Breakdown das mudanças por tipo (usando dictionary comprehension)
mudancas = {
    "Medicamento → Não Medicamento": (
        (F.col("eh_medicamento_standard") == True)
        & (F.col("eh_medicamento_refined") == False)
    ),
    "Não Medicamento → Medicamento": (
        (F.col("eh_medicamento_standard") == False)
        & (F.col("eh_medicamento_refined") == True)
    ),
    "NULL → Medicamento": (
        F.col("eh_medicamento_standard").isNull()
        & (F.col("eh_medicamento_refined") == True)
    ),
    "NULL → Não Medicamento": (
        F.col("eh_medicamento_standard").isNull()
        & (F.col("eh_medicamento_refined") == False)
    ),
    "Medicamento → NULL": (
        (F.col("eh_medicamento_standard") == True)
        & F.col("eh_medicamento_refined").isNull()
    ),
    "Não Medicamento → NULL": (
        (F.col("eh_medicamento_standard") == False)
        & F.col("eh_medicamento_refined").isNull()
    ),
}

print("\n📈 Detalhamento das mudanças:")
for tipo, condicao in mudancas.items():
    count = produtos_que_mudaram.filter(condicao).count()
    print(f"  • {tipo}: {count}")

# COMMAND ----------

# Exibe amostra dos produtos que mudaram
if total_mudancas > 0:
    print("\n📋 Amostra de produtos que tiveram `eh_medicamento` alterado:")
    produtos_que_mudaram.select(
        "ean",
        "nome",
        "eh_medicamento_standard",
        "eh_medicamento_refined",
    ).orderBy(F.rand()).limit(50).display()
else:
    print("\n✅ Nenhum produto teve o valor de `eh_medicamento` alterado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estatísticas antes de salvar

# COMMAND ----------

if DEBUG:
    # Total de produtos
    total_produtos = produtos_final_preparado.select("ean").distinct().count()
    print(f"Total de produtos únicos: {total_produtos}")

    # Produtos com categoria
    produtos_com_cat = (
        produtos_final_preparado.filter(
            F.col("categorias").isNotNull() & (F.size(F.col("categorias")) > 0)
        )
        .select("ean")
        .distinct()
        .count()
    )
    print(f"Produtos com pelo menos uma categoria: {produtos_com_cat}")

    # Produtos sem categoria
    produtos_sem_cat = total_produtos - produtos_com_cat
    print(f"Produtos sem categoria: {produtos_sem_cat}")

    # Distribuição eh_medicamento
    print("\nDistribuição eh_medicamento:")
    produtos_final_preparado.select("ean", "eh_medicamento").distinct().groupBy(
        "eh_medicamento"
    ).count().orderBy("eh_medicamento").display()

    # Distribuição do número de categorias por produto
    print("\nDistribuição do número de categorias por produto:")
    produtos_final_preparado.withColumn(
        "num_categorias", F.size(F.col("categorias"))
    ).groupBy("num_categorias").count().orderBy("num_categorias").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cria tabela Delta se não existir

# COMMAND ----------


env.create_table_if_not_exists(
    Table.categorias_cascata_agregado, schema=agrega_categorias.schema
)

print(
    f"[{agora_em_sao_paulo_str()}] ✅ Tabela Delta criada/verificada: {Table.categorias_cascata_agregado.value}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva na tabela categorias_cascata_agregado

# COMMAND ----------

produtos_final_preparado.write.format("delta").mode("overwrite").saveAsTable(
    Table.categorias_cascata_agregado.value
)

print(
    f"[{agora_em_sao_paulo_str()}] ✅ Dados salvos com sucesso em: {Table.categorias_cascata_agregado.value}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos resultados

# COMMAND ----------

if not DEBUG:
    dbutils.notebook.exit("Sucesso: Dados processados e salvos.")

# COMMAND ----------

categorias_agregado_salvas = env.table(Table.categorias_cascata_agregado).cache()

# COMMAND ----------

print("Validação da tabela salva:")
print(f"Total de registros: {categorias_agregado_salvas.count()}")
print(f"EANs únicos: {categorias_agregado_salvas.select('ean').distinct().count()}")

# COMMAND ----------

print("\nAmostra dos dados salvos:")
categorias_agregado_salvas.orderBy(F.rand()).limit(100).display()

# COMMAND ----------

print("\nDistribuição eh_medicamento na tabela salva:")
categorias_agregado_salvas.select("ean", "eh_medicamento").distinct().groupBy(
    "eh_medicamento"
).count().orderBy("eh_medicamento").display()
