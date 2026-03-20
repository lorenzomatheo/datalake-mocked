# Databricks notebook source
# MAGIC %pip install langchain langchain-core langchain-community langchain_milvus langchain_openai pymilvus openai html2text rapidfuzz langchain_google_genai langchain-google-vertexai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Fuzzy Matching Produtos em Processamento
# MAGIC
# MAGIC **O que faz**:
# MAGIC - Lê de `refined._produtos_em_processamento`
# MAGIC - Identifica produtos (medicamentos e não-medicamentos) com colunas fora do compliance dos enums
# MAGIC - Aplica lógica fuzzy matching para corrigir valores inválidos
# MAGIC - Escreve de volta na mesma tabela
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta import DeltaTable

from maggulake.enums import (
    DescricaoTipoReceita,
    FaixaEtaria,
    FormaFarmaceutica,
    SexoRecomendado,
    TamanhoProduto,
    TipoMedicamento,
    TiposReceita,
    TiposTarja,
    ViaAdministracao,
)
from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.pipelines.process_invalid_compliance_columns import (
    process_invalid_columns,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "fuzzy_matching_produtos_em_processamento",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
catalog = env.settings.catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos Enums

# COMMAND ----------

# NOTE: Como a gente não usar o enum do princípio ativo no enriquecimento e podem existir diversas combinações de moléculas, tenho receio de que possa dar matches incorretos via fuzzy...

# Carrega enum de princípio ativo
# df_p_ativos_enum = env.table(Table.enum_principio_ativo_medicamentos)
# p_ativos_enum: list[Row] = (
#     df_p_ativos_enum.select("principio_ativo").distinct().collect()
# )
# p_ativos_enum: list[str] = [row.principio_ativo for row in p_ativos_enum]
# p_ativos_enum_normalized: list[str] = list(
#     dict.fromkeys(remove_accents(value) for value in p_ativos_enum if value is not None)
# )

# print(f"✅ {len(p_ativos_enum)} princípios ativos carregados do enum")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da Tabela em processamento

# COMMAND ----------

produtos_em_processamento = env.table(Table.produtos_em_processamento).cache()

# Força materialização do cache com count
total_produtos = produtos_em_processamento.count()
print(f"✅ {total_produtos} produtos carregados da tabela em processamento")

# COMMAND ----------

# Cria coluna auxiliar sem acentos apenas para validação das regras
# produtos_em_processamento_normalizado = normalize_column_accent(
#     produtos_em_processamento, "principio_ativo", "principio_ativo_normalized"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação de Produtos Fora do Compliance

# COMMAND ----------

df_produtos_problematicos = produtos_em_processamento.filter(
    # Colunas de medicamentos
    (
        (F.col("eh_medicamento") == True)
        & (~F.col("forma_farmaceutica").isin(FormaFarmaceutica.list()))
        | (~F.col("via_administracao").isin(ViaAdministracao.list()))
        | (~F.col("tipo_receita").isin(TiposReceita.list()))
        | (~F.col("tarja").isin(TiposTarja.list()))
        | (~F.col("tipo_medicamento").isin(TipoMedicamento.list()))
        | (~F.col("tipo_de_receita_completo").isin(DescricaoTipoReceita.list()))
    )
    # Colunas comuns para medicamentos e não-medicamentos
    | (
        (~F.col("idade_recomendada").isin(FaixaEtaria.list()))
        | (~F.col("sexo_recomendado").isin(SexoRecomendado.list()))
        | (~F.col("tamanho_produto").isin(TamanhoProduto.list()))
    )
).drop("principio_ativo_normalized")

# COMMAND ----------

produtos_fora_compliance = df_produtos_problematicos.count()
print(
    f"🔍 {produtos_fora_compliance} produtos identificados com colunas fora do compliance"
)

# COMMAND ----------

if produtos_fora_compliance == 0:
    produtos_em_processamento.unpersist()  # Libera o cache antes de sair
    dbutils.notebook.exit("Nenhum produto fora do compliance encontrado.")

# COMMAND ----------

# Cacheia os produtos problemáticos já que serão processados
# Isso evita recomputação do filtro durante o fuzzy matching
df_produtos_problematicos = df_produtos_problematicos.cache()

problematicos_count = df_produtos_problematicos.count()
print(f"✅ {problematicos_count} produtos problemáticos cacheados para processamento")

# COMMAND ----------

# Define o mapeamento de colunas para enums
column_to_enum = {
    "forma_farmaceutica": FormaFarmaceutica.list(),
    "via_administracao": ViaAdministracao.list(),
    "idade_recomendada": FaixaEtaria.list(),
    "sexo_recomendado": SexoRecomendado.list(),
    "tipo_receita": TiposReceita.list(),
    "tarja": TiposTarja.list(),
    "tipo_medicamento": TipoMedicamento.list(),
    "tipo_de_receita_completo": DescricaoTipoReceita.list(),
    "tamanho_produto": TamanhoProduto.list(),
}

# COMMAND ----------

# Registra a função de processamento como UDF
# NOTE: Daria pra usar o F.levenshtein entre dois termos tbm, só curiosidade
process_invalid_columns_udf = F.udf(
    lambda row: process_invalid_columns(row, column_to_enum),
    T.MapType(T.StringType(), T.StringType()),
)

# COMMAND ----------

# Aplica fuzzy matching para corrigir valores inválidos
# NOTE: Esta é a etapa mais custosa - o UDF roda fuzzy matching em Python
df_produtos_corrigidos = df_produtos_problematicos.withColumn(
    "corrected_values",
    process_invalid_columns_udf(F.struct(*df_produtos_problematicos.columns)),
)

# COMMAND ----------

# Atualiza as colunas com os valores corrigidos
for field in column_to_enum:
    df_produtos_corrigidos = df_produtos_corrigidos.withColumn(
        field, F.coalesce(F.col(f"corrected_values.{field}"), F.col(field))
    )

# Remove a coluna auxiliar de valores corrigidos
df_produtos_corrigidos = df_produtos_corrigidos.drop("corrected_values")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialização dos Produtos Corrigidos
# MAGIC
# MAGIC Esta célula força a execução do fuzzy matching e salva o resultado em uma
# MAGIC tabela temporária. Isso evita que todo o processamento seja feito durante
# MAGIC o salvamento final, permitindo monitorar o progresso e evitar timeouts.

# COMMAND ----------

# Salva produtos corrigidos em tabela temporária para materializar o processamento
# Isso evita que o Spark acumule toda a computação para o write final
temp_table_name = f"{catalog}.refined._produtos_corrigidos_temp"

# TODO: overwriteSchema pode quebrar schema de produção. Avaliar se é necessário.
df_produtos_corrigidos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(temp_table_name)

print(f"✅ Produtos corrigidos salvos em tabela temporária: {temp_table_name}")

# COMMAND ----------

# Lê de volta e cacheia para validação e merge
df_produtos_corrigidos = spark.read.table(temp_table_name).cache()
corrigidos_count = df_produtos_corrigidos.count()
print(f"✅ {corrigidos_count} produtos corrigidos materializados e cacheados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação: Valores Únicos por Coluna

# COMMAND ----------

# Exibe valores únicos das colunas com enum para validação visual
# NOTE: df_produtos_corrigidos já está cacheado, então essas operações são rápidas
print(f"Total de produtos analisados: {total_produtos}")
print(f"Produtos fora do compliance: {produtos_fora_compliance}")
print(f"Produtos corrigidos: {corrigidos_count}")
print("\n" + "=" * 80)

for field in column_to_enum:
    print(f"\n📋 Coluna: {field}")
    print("-" * 80)

    # Pega valores únicos da coluna após correção
    valores_unicos = (
        df_produtos_corrigidos.select(field)
        .distinct()
        .orderBy(F.col(field).asc_nulls_last())
        .collect()
    )

    # Exibe os valores
    for row in valores_unicos:
        valor = row[field]
        if valor is None:
            print("  - 'None'")
        else:
            print(f"  - {valor}")

    print(f"\nTotal: {len(valores_unicos)} valores únicos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Tabela via MERGE

# COMMAND ----------

# Usa MERGE por ser mais eficiente que fazer join + union + overwrite completo
delta_table = DeltaTable.forName(spark, Table.produtos_em_processamento.value)

# COMMAND ----------

# Seleciona apenas as colunas necessárias para o merge (evita problemas de schema)
colunas_para_atualizar = list(column_to_enum.keys())

# COMMAND ----------

# Cria o dicionário de colunas a serem atualizadas
update_columns = {col: F.col(f"source.{col}") for col in colunas_para_atualizar}

# COMMAND ----------

delta_table.alias("target").merge(
    df_produtos_corrigidos.alias("source"),
    "target.ean = source.ean",
).whenMatchedUpdate(set=update_columns).execute()

print(f"✅ MERGE executado com sucesso em {Table.produtos_em_processamento.value}")
print(f"   {corrigidos_count} registros atualizados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza de Recursos

# COMMAND ----------

# Libera caches para liberar memória
df_produtos_problematicos.unpersist()
df_produtos_corrigidos.unpersist()
produtos_em_processamento.unpersist()

print("✅ Recursos liberados e tabela temporária removida")

# COMMAND ----------

# TODO: Acho que vale salvar essas correções no delta table para conseguirmos
# ter esse histórico e em algum momento conseguir até otimizar os enriquecimentos

# Remove tabela temporária
spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")

print(f"✅ Tabela {Table.produtos_em_processamento.value} atualizada com sucesso")
