# Databricks notebook source
# MAGIC %md
# MAGIC # Completar as informações de Idade e Sexo
# MAGIC
# MAGIC **O que faz**:
# MAGIC - Lê de `refined._produtos_em_processamento`
# MAGIC - Preenche os campos de idade e sexo para produtos com base em categorias, indicação, nome e descrição
# MAGIC - Utiliza regras regex e enumerações para atribuir valores apropriados
# MAGIC - Escreve de volta na mesma tabela via MERGE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Inicial

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from maggulake.enums import (
    FaixaEtaria,
    SexoRecomendado,
)
from maggulake.environment import DatabricksEnvironmentBuilder, Table

# COMMAND ----------

# Inicializar environment
env = DatabricksEnvironmentBuilder.build(
    "completar_idade_e_sexo",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)
spark = env.spark
catalog = env.settings.catalog

# DEBUG flag
DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura da Tabela em Processamento

# COMMAND ----------

# Lê produtos em processamento e cacheia
produtos_em_processamento = env.table(Table.produtos_em_processamento).cache()

# Força materialização do cache com count
total_produtos = produtos_em_processamento.count()
print(f"✅ {total_produtos} produtos carregados da tabela em processamento")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação de Produtos para Processar

# COMMAND ----------

# Filtra produtos com campos nulos ou vazios que precisam ser preenchidos
produtos_para_processar = produtos_em_processamento.filter(
    (F.col("idade_recomendada").isNull() | (F.col("idade_recomendada") == ""))
    | (F.col("sexo_recomendado").isNull() | (F.col("sexo_recomendado") == ""))
).cache()

# COMMAND ----------

produtos_para_processar_count = produtos_para_processar.count()
print(f"🔍 {produtos_para_processar_count} produtos identificados para processar")

# COMMAND ----------

if produtos_para_processar_count == 0:
    produtos_em_processamento.unpersist()  # Libera o cache antes de sair
    dbutils.notebook.exit("Nenhum produto para processar encontrado.")

# COMMAND ----------

if DEBUG:
    print("Amostra de produtos a processar:")
    produtos_para_processar.select(
        "ean", "nome", "idade_recomendada", "sexo_recomendado", "categorias"
    ).limit(10).display()

# COMMAND ----------


def preencher_idade_e_sexo_por_categorias(df: DataFrame) -> DataFrame:
    """
    Visão geral da lógica:
    - Constrói uma string única `search_text` com nome, descrição, indicação e categorias em minúsculas.
    - Usa regex em `search_text` para inferir `idade_recomendada` e `sexo_recomendado`.
    - Sempre prioriza segurança:
        - Em caso de negação/precaução ("não indicado para crianças", "mantenha fora do alcance...") -> ADULTO.
        - Só atribui BEBE/CRIANCA/IDOSO quando há indicação clara e específica para essa faixa etária.
    - Se houver dúvida ou público misto, mantém ADULTO/TODOS em vez de forçar uma faixa etária restrita.
    """

    # Criar search_text concatenando todas as fontes em minúsculas
    search_text = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("nome"), F.lit("")),
            F.coalesce(F.col("descricao"), F.lit("")),
            F.coalesce(F.col("indicacao"), F.lit("")),
            # Tratar categorias como array e concatenar
            F.when(
                F.col("categorias").isNotNull(), F.concat_ws(" ", F.col("categorias"))
            ).otherwise(F.lit("")),
        )
    )

    # Regras de idade_recomendada
    tem_indicacao_crianca_clara = search_text.rlike(
        "infantil|pediatrico|para crian[çc]a(s)?|crian[çc]a(s)?.*para|cuidado.*crian[çc]a(s)?|crian[çc]a(s)?.*cuidado"
    )

    # Detectar negações explícitas para idade
    tem_negacao_idade = search_text.rlike(
        "n[ãa]o\\s*recomendad[ao].*crian[çc]a(s)?|n[ãa]o\\s*recomendad[ao].*beb[êe]s?|"
        + "n[ãa]o\\s*.*indicado.*crian[çc]a(s)?|n[ãa]o\\s*.*indicado.*beb[êe]s?|"
        + "evite.*crian[çc]a(s)?|evite.*beb[êe]s?|"
        + "contraindicado.*crian[çc]a(s)?|contraindicado.*beb[êe]s?|"
        + "mantenha.*alcance.*crian[çc]a(s)?|mantenha fora do alcance.*crian[çc]a(s)?|"
        + "mantenha.*longe.*crian[çc]a(s)?|"
        + "manter.*alcance.*crian[çc]a(s)?|manter fora do alcance.*crian[çc]a(s)?|"
        + "manter.*longe.*crian[çc]a(s)?|"
        + "evite.*crian[çc]a(s)?"
    )

    # Indicação clara de produto infantil
    tem_crianca_indicacao = tem_indicacao_crianca_clara | (
        search_text.rlike("3-12 anos") & ~tem_negacao_idade
    )

    # Detectar indicação explícita de uso adulto
    tem_adulto_indicacao = search_text.rlike(
        "uso adulto|para adultos?|produto adulto|publico adulto|p[úu]blico adulto"
    )

    # Expressão final de idade_recomendada
    # Ordem de prioridade:
    #    1. Negações/precauções -> ADULTO
    #    2. BEBE   -> termos como "bebê", "bebês", "recém-nascido", "0-2 anos"
    #    3. CRIANCA -> termos infantis fortes ou faixa "3-12 anos"
    #    4. ADOLESCENTE -> "adolescente" ou "13-17 anos"
    #    5. IDOSO  -> "idoso", "sênior", "terceira idade", "60+", "65+"
    #    6. ADULTO -> "uso adulto", "para adultos", etc.
    #    7. Caso nada disso se aplique -> TODOS (fallback conservador).

    idade_expr = (
        F.when(
            tem_negacao_idade,
            F.lit(FaixaEtaria.ADULTO.value),  # Se há negação, retornar ADULTO
        )
        .when(
            search_text.rlike(
                "\\bbeb[êe]s?\\b|recém-nascido|recem-nascido|0-2 anos|chupeta|mamadeira"
            ),
            F.lit(FaixaEtaria.BEBE.value),
        )
        .when(
            tem_crianca_indicacao,
            F.lit(FaixaEtaria.CRIANCA.value),
        )
        .when(
            search_text.rlike("\\badolescente\\b|13-17 anos"),
            F.lit(FaixaEtaria.ADOLESCENTE.value),
        )
        .when(
            search_text.rlike("\\bidoso\\b|\\bs[êe]nior\\b|terceira idade|60\\+|65\\+"),
            F.lit(FaixaEtaria.IDOSO.value),
        )
        .when(
            tem_adulto_indicacao,
            F.lit(FaixaEtaria.ADULTO.value),
        )
        .otherwise(F.lit(FaixaEtaria.TODOS.value))
    )

    # Definir expressões para sexo_recomendado
    tem_mulher_base = search_text.rlike(
        "mulher|mulheres|feminino|gestante|gravidez|menopausa"
    )
    tem_homem_base = search_text.rlike("homem|homens|masculino|barba|barbear|prostata")
    # Detectar indicadores no NOME para produtos sem palavras-chave explícitas
    nome_lower = F.lower(F.col("nome"))
    nome_feminino = nome_lower.rlike(
        "esmalte|maquiagem|batom|rímel|sombra|blush|feminino|mulher|rosa|pink"
    )
    nome_masculino = nome_lower.rlike("barba|barbear|masculino|homem|homens")
    nome_infantil = nome_lower.rlike("kids|baby|infantil|crianca|bebe")

    # Aplicar combinação com nome
    tem_mulher = tem_mulher_base | nome_feminino
    tem_homem = tem_homem_base | nome_masculino

    # Combinar indicação de texto com nome para CRIANÇA
    tem_crianca_indicacao = tem_crianca_indicacao | nome_infantil

    # Detectar negações explícitas para cada sexo
    tem_negacao_mulher = search_text.rlike(
        "nao recomendado.*mulher|evite.*mulher|contraindicado.*mulher"
    )
    tem_negacao_homem = search_text.rlike(
        "nao recomendado.*homem|evite.*homem|contraindicado.*homem"
    )

    # Expressão final de sexo_recomendado
    # Ordem de prioridade:
    # 1. Negação explícita de mulher -> HOMEM
    # 2. Negação explícita de homem -> MULHER
    # 3. Menção a ambos os sexos -> TODOS
    # 4. Menção apenas a mulher -> MULHER
    # 5. Menção apenas a homem -> HOMEM
    # 6. Default -> TODOS
    sexo_expr = (
        F.when(
            tem_negacao_mulher,  # Mulher explicitamente contraindicada -> HOMEM
            F.lit(SexoRecomendado.HOMEM.value),
        )
        .when(
            tem_negacao_homem,  # Homem explicitamente contraindicado -> MULHER
            F.lit(SexoRecomendado.MULHER.value),
        )
        .when(
            tem_mulher & tem_homem,  # AMBOS mencionados
            F.lit(SexoRecomendado.TODOS.value),
        )
        .when(
            tem_mulher & ~tem_homem,  # Apenas MULHER
            F.lit(SexoRecomendado.MULHER.value),
        )
        .when(
            tem_homem & ~tem_mulher,  # Apenas HOMEM
            F.lit(SexoRecomendado.HOMEM.value),
        )
        .otherwise(F.lit(SexoRecomendado.TODOS.value))
    )

    # 4. Aplicar as expressões apenas onde os campos são nulos ou inválidos
    return (
        df.withColumn("search_text", search_text)
        .withColumn(
            "idade_recomendada_new",
            F.when(
                (F.col("idade_recomendada").isNull())
                | (F.lower(F.col("idade_recomendada")) == ""),
                idade_expr,
            ).otherwise(F.col("idade_recomendada")),
        )
        .withColumn(
            "sexo_recomendado_new",
            F.when(
                (F.col("sexo_recomendado").isNull())
                | (F.lower(F.col("sexo_recomendado")) == ""),
                sexo_expr,
            ).otherwise(F.col("sexo_recomendado")),
        )
        .withColumn("idade_recomendada", F.col("idade_recomendada_new"))
        .withColumn("sexo_recomendado", F.col("sexo_recomendado_new"))
        .drop("search_text", "idade_recomendada_new", "sexo_recomendado_new")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação do Enriquecimento

# COMMAND ----------

# Aplica enriquecimento de idade e sexo via regex
# NOTE: Esta é a etapa de processamento principal
produtos_corrigidos = preencher_idade_e_sexo_por_categorias(produtos_para_processar)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialização dos Produtos Corrigidos
# MAGIC
# MAGIC Esta célula força a execução do processamento regex e salva o resultado em uma
# MAGIC tabela temporária. Isso evita que todo o processamento seja feito durante
# MAGIC o salvamento final, permitindo monitorar o progresso e evitar timeouts.

# COMMAND ----------

# Salva produtos corrigidos em tabela temporária para materializar o processamento
# Isso evita que o Spark acumule toda a computação para o write final mesmo
temp_table_name = f"{catalog}.refined._produtos_idade_sexo_temp"

produtos_corrigidos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(temp_table_name)

print(f"✅ Produtos corrigidos salvos em tabela temporária: {temp_table_name}")

# COMMAND ----------

# Lê de volta e cacheia para validação e merge
produtos_corrigidos = spark.read.table(temp_table_name).cache()
corrigidos_count = produtos_corrigidos.count()
print(f"✅ {corrigidos_count} produtos corrigidos materializados e cacheados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação: Amostra de Valores Corrigidos

# COMMAND ----------

if DEBUG:
    print(f"Total de produtos analisados: {total_produtos}")
    print(f"Produtos para processar: {produtos_para_processar_count}")
    print(f"Produtos corrigidos: {corrigidos_count}")
    print("\n" + "=" * 80)

    print("\n📋 Amostra de produtos corrigidos:")
    print("-" * 80)
    produtos_corrigidos.select(
        "ean", "nome", "idade_recomendada", "sexo_recomendado"
    ).limit(10).display()

    print("\n📊 Distribuição de idade_recomendada:")
    produtos_corrigidos.groupBy("idade_recomendada").count().orderBy(
        F.desc("count")
    ).display()

    print("\n📊 Distribuição de sexo_recomendado:")
    produtos_corrigidos.groupBy("sexo_recomendado").count().orderBy(
        F.desc("count")
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento na Tabela via MERGE

# COMMAND ----------

# Usa MERGE por ser mais eficiente que fazer join + union + overwrite completo
delta_table = DeltaTable.forName(spark, Table.produtos_em_processamento.value)

# COMMAND ----------

# Cria o dicionário de colunas a serem atualizadas
update_columns = {
    "idade_recomendada": F.col("source.idade_recomendada"),
    "sexo_recomendado": F.col("source.sexo_recomendado"),
}

# COMMAND ----------

delta_table.alias("target").merge(
    produtos_corrigidos.alias("source"),
    "target.ean = source.ean",
).whenMatchedUpdate(set=update_columns).execute()

print(f"✅ MERGE executado com sucesso em {Table.produtos_em_processamento.value}")
print(f"   {corrigidos_count} registros atualizados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza de Recursos

# COMMAND ----------

# Libera caches para liberar memória
produtos_para_processar.unpersist()
produtos_corrigidos.unpersist()
produtos_em_processamento.unpersist()

print("✅ Caches liberados")

# COMMAND ----------

# Remove tabela temporária
spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")

print("✅ Tabela temporária removida")
print(f"✅ Tabela {Table.produtos_em_processamento.value} atualizada com sucesso")
