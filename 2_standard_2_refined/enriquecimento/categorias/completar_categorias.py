# Databricks notebook source
# MAGIC %md
# MAGIC # Atualiza Coluna Categorias via Google Sheets
# MAGIC
# MAGIC Este notebook lê categorias de um Google Sheets e atualiza a coluna `categorias`
# MAGIC dos produtos na tabela `_produtos_em_processamento` através de matching direto
# MAGIC entre os termos do Sheets e o nome/princípio ativo dos produtos que ainda não possuem categorias.
# MAGIC
# MAGIC **Lógica**:
# MAGIC - **MEDICAMENTOS** (Macrocategoria = "Medicamentos"):
# MAGIC   - Termo deve estar presente no NOME **E** no PRINCÍPIO ATIVO
# MAGIC - **NÃO MEDICAMENTOS** (outras Macrocategorias):
# MAGIC   - Termo deve estar presente apenas no NOME
# MAGIC - Produtos sem match mantêm suas categorias originais

# COMMAND ----------

# MAGIC %pip install gspread google-auth

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json

import gspread
import pyspark.sql.functions as F
import pyspark.sql.types as T
from delta.tables import DeltaTable
from google.oauth2.service_account import Credentials

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.utils.strings import remove_accents
from maggulake.utils.text_similarity import tokenize_for_matching
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Environment and Widgets

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "atualiza_coluna_categorias",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
)

spark = env.spark
catalog = env.settings.catalog

TAB_NAME = "categorias"
DEBUG = dbutils.widgets.get("debug") == "true"

# COMMAND ----------

print(f"[{agora_em_sao_paulo_str()}] ⚙️  Configuração:")
print(f"  - Catalog: {catalog}")
print(f"  - Tab name: {TAB_NAME or '(todas as abas)'}")
print(f"  - Debug: {DEBUG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurações de Matching

# COMMAND ----------

# Tamanho mínimo de caracteres para termos (previne false positives de termos curtos)
TAMANHO_MINIMO_TERMO = 5

# Blocklist de termos ambíguos (sufixos genéricos que não devem fazer match sozinhos)
TERMOS_BLOCKLIST = [
    # Sufixos farmacológicos genéricos
    "CONAZOL",
    "PRAZOL",
    "ZOLAM",
    "AFAX",
    "ANDES",
    "ZINA",
    "ROXETIN",
    "PAROX",
    "IMIPRA",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Google Sheets Configuration

# COMMAND ----------

json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

gc = gspread.authorize(creds)


SHEET_ID = "1Z3s7jJD4i4iAwc0ahObouNlkXX--K3iGOZXGN8t3HYw"

print(f"[{agora_em_sao_paulo_str()}] 🔗 Conectando ao Google Sheets ID: {SHEET_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Normalização

# COMMAND ----------


# UDF para remove_accents
remove_accents_udf = F.udf(remove_accents, T.StringType())

# UDF para tokenização
tokenize_for_matching_udf = F.udf(
    lambda nome, pa: tokenize_for_matching(
        nome,
        pa,
        tamanho_minimo=TAMANHO_MINIMO_TERMO,
        termos_blocklist=TERMOS_BLOCKLIST,
        max_termos_nome=2,
        max_termos_pa=3,
    ),
    T.ArrayType(T.StringType()),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do Google Sheets

# COMMAND ----------

spreadsheet = gc.open_by_key(SHEET_ID)

tab_to_process = [t.strip() for t in TAB_NAME.split(",") if t.strip()]

print(f"[{agora_em_sao_paulo_str()}] 📋 Abas a processar: {tab_to_process}")

all_rows = []
for tab in tab_to_process:
    print(f"[{agora_em_sao_paulo_str()}] 📖 Lendo aba: {tab}")
    rows = spreadsheet.worksheet(tab).get_all_records()

    for row in rows:
        # Garantir que campos críticos sejam strings
        row["Hierarquia"] = (
            str(row.get("Hierarquia", "")) if row.get("Hierarquia") else None
        )
        row["Macrocategoria"] = (
            str(row.get("Macrocategoria", "")) if row.get("Macrocategoria") else None
        )
        row["termos"] = str(row.get("termos", "")) if row.get("termos") else None

    all_rows.extend(rows)

print(f"[{agora_em_sao_paulo_str()}] ✅ Total de linhas lidas: {len(all_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento dos Termos do Sheets

# COMMAND ----------

# Criar DataFrame inicial
df_sheets = spark.createDataFrame(all_rows)

# Selecionar e limpar colunas
df_sheets = df_sheets.select(
    F.col("Hierarquia").cast(T.StringType()).alias("hierarquia"),
    F.col("Macrocategoria").cast(T.StringType()).alias("macrocategoria"),
    F.col("termos").cast(T.StringType()).alias("termos_json"),
)

# Filtrar linhas válidas
df_sheets = df_sheets.filter(
    F.col("hierarquia").isNotNull()
    & (F.col("hierarquia") != "")
    & F.col("macrocategoria").isNotNull()
    & (F.col("macrocategoria") != "")
    & F.col("termos_json").isNotNull()
    & (F.col("termos_json") != "")
)

print(
    f"[{agora_em_sao_paulo_str()}] 📊 Linhas válidas após filtro: {df_sheets.count()}"
)

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Amostra do Sheets (primeiras 5 linhas):")
    display(df_sheets.limit(5))

# COMMAND ----------

# Parsear JSON e explodir termos
# Schema para o array de termos
termos_schema = T.ArrayType(T.StringType())

df_termos = df_sheets.withColumn(
    "termos_array", F.from_json(F.col("termos_json"), termos_schema)
)

# Explodir termos em linhas individuais
df_termos_exploded = df_termos.select(
    F.col("hierarquia"),
    F.col("macrocategoria"),
    F.explode(F.col("termos_array")).alias("termo"),
).withColumn(
    "termo",
    F.trim(F.col("termo")),  # Remove espaços em branco
)

# Normalizar termos para matching (remove acentos, uppercase, trim)
df_termos_exploded = df_termos_exploded.withColumn(
    "termo_normalizado", F.trim(F.upper(remove_accents_udf(F.col("termo"))))
)

# Remover termos vazios
df_termos_exploded = df_termos_exploded.filter(
    F.col("termo_normalizado").isNotNull() & (F.col("termo_normalizado") != "")
)

# Remover duplicatas
df_termos_lookup = df_termos_exploded.select(
    "termo_normalizado", "hierarquia", "macrocategoria"
).distinct()

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Amostra de termos (primeiros 20):")
    display(df_termos_lookup.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros de Validação de Termos

# COMMAND ----------

# Filtrar termos muito curtos (prevenir false positives)

termos_antes_filtro_tamanho = df_termos_lookup.count()

df_termos_lookup = df_termos_lookup.filter(
    F.length(F.col("termo_normalizado")) >= TAMANHO_MINIMO_TERMO
)

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 🔍 Exemplos de termos removidos (muito curtos):"
    )
    display(
        df_termos_exploded.filter(
            F.length(F.col("termo_normalizado")) < TAMANHO_MINIMO_TERMO
        )
        .select("termo", "termo_normalizado", "hierarquia")
        .distinct()
        .limit(20)
    )

# COMMAND ----------

# Remover termos da blocklist
termos_antes_blocklist = df_termos_lookup.count()

# Criar DataFrame da blocklist para fazer anti-join
df_blocklist = spark.createDataFrame(
    [(termo,) for termo in TERMOS_BLOCKLIST], ["termo_blocklist"]
)

df_termos_lookup = df_termos_lookup.join(
    df_blocklist,
    F.col("termo_normalizado") == F.col("termo_blocklist"),
    "left_anti",  # mantém apenas termos que NÃO estão na blocklist
)

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 🔍 Exemplos de termos removidos pela blocklist:"
    )
    display(
        df_termos_exploded.join(
            df_blocklist,
            F.col("termo_normalizado") == F.col("termo_blocklist"),
            "inner",
        )
        .select("termo", "termo_normalizado", "hierarquia")
        .distinct()
        .limit(20)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento e Preparação dos Produtos

# COMMAND ----------

# Somente produtos sem categorias
produtos = (
    env.table(Table.produtos_em_processamento)
    .filter(F.col("categorias").isNull() | (F.size(F.col("categorias")) == 0))
    .cache()
)
total_produtos = produtos.count()

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 🔍 Amostra de produtos em processamento (primeiros 5):"
    )
    display(produtos.limit(5))

# COMMAND ----------

# Preparar colunas para matching
produtos_prep = produtos.select(
    F.col("ean"),
    F.col("nome"),
    F.col("principio_ativo"),
    F.col("categorias").alias("categorias_originais"),
    F.col("eh_medicamento"),
).cache()

# Materializar cache
produtos_prep.count()
print(f"[{agora_em_sao_paulo_str()}] ✅ Cache de produtos preparados materializado")

if DEBUG:
    print(
        f"[{agora_em_sao_paulo_str()}] 🔍 Amostra de produtos preparados (primeiros 5):"
    )
    display(
        produtos_prep.select(
            "ean",
            "nome",
            "principio_ativo",
        ).limit(5)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching de Termos com Produtos
# MAGIC
# MAGIC #### 1. Tokenização Inteligente
# MAGIC - Extrai palavras individuais de nome/princípio ativo
# MAGIC - Remove automaticamente números e unidades (500MG, 30ML)
# MAGIC - Mantém apenas tokens com ≥5 caracteres
# MAGIC - Aplica blocklist de termos ambíguos
# MAGIC
# MAGIC #### 2. Word Boundaries Preservados
# MAGIC - Tokenização com regex `\b\w+\b` garante palavras completas
# MAGIC - ❌ "PRAZOL" não faz match em "ALPRAZOLAM" (são tokens diferentes)
# MAGIC - ✅ "ALPRAZOLAM" faz match em "ALPRAZOLAM 0,25MG"
# MAGIC
# MAGIC #### 3. Separação por Tipo de Produto:
# MAGIC - **Medicamentos**: Tokeniza nome + princípio ativo
# MAGIC - **Não Medicamentos**: Tokeniza apenas nome

# COMMAND ----------

# Separar termos por tipo
termos_medicamentos = df_termos_lookup.filter(F.col("macrocategoria") == "Medicamentos")
termos_nao_medicamentos = df_termos_lookup.filter(
    F.col("macrocategoria") != "Medicamentos"
)

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Amostra termos medicamentos:")
    display(termos_medicamentos.limit(10))
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Amostra termos não medicamentos:")
    display(termos_nao_medicamentos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criar Lookup de Termos Expandido

# COMMAND ----------

# tabela de lookup expandida
df_termos_lookup_expanded = (
    df_termos_lookup.select(
        F.col("termo_normalizado").alias("token"),
        F.col("hierarquia"),
        F.col("macrocategoria"),
    )
    .withColumn(
        "is_medicamento",
        F.when(F.col("macrocategoria") == "Medicamentos", True).otherwise(False),
    )
    .select("token", "hierarquia", "is_medicamento")
    .distinct()
    .cache()  # Cache pois será usado 2x
)

# Materializar o cache
total_termos_lookup = df_termos_lookup_expanded.count()
print(
    f"[{agora_em_sao_paulo_str()}] ✅ Lookup de termos: {total_termos_lookup} tokens únicos"
)

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Amostra do lookup de termos:")
    display(df_termos_lookup_expanded.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ETAPA 1: Tokenização e Matching para Medicamentos

# COMMAND ----------

# Tokenizar produtos considerando nome E princípio ativo
produtos_tokens_med = (
    produtos_prep.filter(F.col('eh_medicamento') == True)
    .withColumn(
        "tokens_nome",
        tokenize_for_matching_udf(
            F.col("nome"),
            F.lit(None),  # só nome
        ),
    )
    .withColumn(
        "tokens_pa",
        tokenize_for_matching_udf(
            F.lit(None),
            F.col("principio_ativo"),  # só princípio ativo
        ),
    )
    .withColumn(
        "tokens_array", F.array_intersect(F.col("tokens_nome"), F.col("tokens_pa"))
    )
    .select("ean", F.explode("tokens_array").alias("token"))
    .distinct()  # Remove duplicatas de tokens por EAN
)

# Join com termos de medicamentos usando broadcast
termos_med_broadcast = F.broadcast(
    df_termos_lookup_expanded.filter(F.col("is_medicamento") == True)
)

medicamentos_match = (
    produtos_tokens_med.join(termos_med_broadcast, how="inner", on=['token'])
    .select("ean", "hierarquia")
    .distinct()
)

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Exemplos de matches medicamentos:")
    display(medicamentos_match.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ETAPA 2: Tokenização e Matching para Não Medicamentos

# COMMAND ----------

# Tokenizar produtos considerando apenas o nome
produtos_tokens_nao_med = (
    produtos_prep.filter(F.col('eh_medicamento') == False)
    .withColumn(
        "tokens_array",
        tokenize_for_matching_udf(
            F.col("nome"),
            F.lit(None),  # Ignora princípio ativo para não-medicamentos
        ),
    )
    .select("ean", F.explode("tokens_array").alias("token"))
    .distinct()
)

# Join com termos de não-medicamentos usando broadcast
termos_nao_med_broadcast = F.broadcast(
    df_termos_lookup_expanded.filter(F.col("is_medicamento") == False)
)

nao_medicamentos_match = (
    produtos_tokens_nao_med.join(termos_nao_med_broadcast, how="inner", on=['token'])
    .select("ean", "hierarquia")
    .distinct()
)

if DEBUG:
    print(f"[{agora_em_sao_paulo_str()}] 🔍 Exemplos de matches não medicamentos:")
    display(nao_medicamentos_match.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### União dos Resultados

# COMMAND ----------

# Union dos dois tipos de match
produtos_com_match = medicamentos_match.unionAll(nao_medicamentos_match)

if not produtos_com_match.take(1):
    dbutils.notebook.exit(
        "DataFrame de produtos com match está vazio e não será salvo."
    )

# COMMAND ----------

# Contagens
produtos_com_match_eans = produtos_com_match.select("ean").distinct()
total_com_match = produtos_com_match_eans.count()
total_sem_match = total_produtos - total_com_match

print(f"\n[{agora_em_sao_paulo_str()}] 📊 RESUMO GERAL:")
print(f"  - Total de produtos: {total_produtos:,}")
print(f"  - Total COM match: {total_com_match:,}")
print(f"  - Total SEM match (mantidos): {total_sem_match:,}")
print(f"  - Percentual de atualização: {(total_com_match / total_produtos * 100):.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando Resultados

# COMMAND ----------

# Agregar categorias por EAN
categorias_agregadas = produtos_com_match.groupBy(
    "ean",
).agg(F.collect_set("hierarquia").alias("categorias_novas"))

# Contar produtos atualizados para estatísticas
produtos_atualizados_count = categorias_agregadas.count()

print(
    f"\n[{agora_em_sao_paulo_str()}] 🔄 Executando MERGE para atualizar categorias..."
)
print(f"   Total de produtos a serem atualizados: {produtos_atualizados_count:,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar MERGE para atualizar apenas produtos com match

# COMMAND ----------

# Obter Delta Table
delta_table = DeltaTable.forName(spark, Table.produtos_em_processamento.value)

# COMMAND ----------

# Definir colunas a serem atualizadas
update_columns = {
    "categorias": F.col("source.categorias_novas"),
}

# COMMAND ----------

# Executar MERGE: atualiza apenas produtos que tiveram match
delta_table.alias("target").merge(
    categorias_agregadas.alias("source"),
    "target.ean = source.ean",
).whenMatchedUpdate(set=update_columns).execute()

print(
    f"\n[{agora_em_sao_paulo_str()}] ✅ MERGE executado com sucesso em {Table.produtos_em_processamento.value}"
)
print(f"   {produtos_atualizados_count:,} produtos tiveram suas categorias atualizadas")
