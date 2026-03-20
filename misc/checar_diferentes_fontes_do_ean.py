# Databricks notebook source
# MAGIC %md
# MAGIC # Checar diferentes fontes do EAN
# MAGIC
# MAGIC Este notebook serve para verificar a ingestão e enriquecimento de dados de um produto específico.
# MAGIC
# MAGIC **Objetivo:** Carregar todas as tabelas de todas as camadas (raw, standard, refined e enriquecimento) e filtrar para um EAN específico. Assim podemos analisar o valor de cada coluna em cada uma das fontes/camadas.
# MAGIC
# MAGIC **Útil para:**
# MAGIC - Debug de data quality
# MAGIC - Rastreamento de data lineage
# MAGIC - Identificação de conflitos entre fontes
# MAGIC - Validação de enriquecimentos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json

import pyspark.sql.functions as F
import pyspark.sql.types as T

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.mappings.anvisa import AnvisaParser
from maggulake.mappings.farmarcas import FarmarcasParser
from maggulake.mappings.iqvia import IqviaParser
from maggulake.mappings.minas_mais import MinasMaisParser
from maggulake.mappings.RD import RdParser

# TODO: ficou faltando o parser do Consulta Remédios (CR), vou ver isso depois
# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

# Configurar widgets
env = DatabricksEnvironmentBuilder.build(
    "checar_diferentes_fontes_do_ean",
    dbutils,
    widgets={"ean": ""},
)

stage = env.settings.name_short
catalog = env.settings.catalog
spark = env.spark

# COMMAND ----------

# Obter EAN do widget
EAN = dbutils.widgets.get("ean").strip()

if not EAN:
    raise ValueError("❌ Por favor, forneça um EAN no widget 'ean'")

print(f"🔍 Buscando informações para o EAN: {EAN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir colunas para comparação

# COMMAND ----------

# NOTE: preferi deixar "bula" de fora, pois é um texto muito grande e não faz muita diferença entre fontes.
# NOTE: não adicionei tags num primeiro momento, mas poderia ser analisado no futuro.
COLUNAS_PARA_CHECAR = [
    "ean",
    "nome",
    "marca",
    "fabricante",
    "marca_websearch",
    "fabricante_websearch",
    "principio_ativo",
    "dosagem",
    "descricao",
    "eh_medicamento",
    "eh_tarjado",
    "eh_controlado",
    "eh_generico",
    "eh_otc",
    "tipo_medicamento",
    "tarja",
    "tipo_receita",
    "tipo_de_receita_completo",
    "categorias",
    "classes_terapeuticas",
    "especialidades",
    "forma_farmaceutica",
    "via_administracao",
    "volume_quantidade",
    "tamanho_produto",
    "indicacao",
    "idade_recomendada",
    "sexo_recomendado",
    # "imagem_url",
    "power_phrase",
    "fonte",
    "atualizado_em",
    "gerado_em",
]

# Colunas que são arrays e não devem ser comparadas por igualdade
COLUNAS_ARRAY_SEM_COMPARACAO = [
    "categorias",
    "classes_terapeuticas",
    "especialidades",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar tabelas com filtro no EAN

# COMMAND ----------

print("📥 Carregando tabelas...")

tables_to_load = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabelas principais (camadas medallion)

# COMMAND ----------

# MAGIC %md
# MAGIC #### produtos_refined

# COMMAND ----------

df = env.table(Table.produtos_refined).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["produtos_refined"] = df
print(f"produtos_refined: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### produtos_em_processamento

# COMMAND ----------

df = env.table(Table.produtos_em_processamento).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["produtos_em_processamento"] = df
print(f"produtos_em_processamento: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### produtos_standard

# COMMAND ----------

df = env.table(Table.produtos_standard).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["produtos_standard"] = df
print(f"produtos_standard: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### produtos_raw

# COMMAND ----------

df = env.table(Table.produtos_raw).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["produtos_raw"] = df
print(f"produtos_raw: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### produtos_genericos

# COMMAND ----------

df = env.table(Table.produtos_genericos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["produtos_genericos"] = df
print(f"produtos_genericos: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabelas de enriquecimento

# COMMAND ----------

# MAGIC %md
# MAGIC #### gpt_enriquece_bula (medicamentos)

# COMMAND ----------

df = env.table(Table.extract_product_info).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["gpt_extract_product_info"] = df
print(f"gpt_extract_product_info: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### extract_product_info_regex

# COMMAND ----------

df = env.table(Table.extract_product_info_regex).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["gpt_extract_product_info_regex"] = df
print(f"gpt_extract_product_info_regex: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### extract_product_info_nao_medicamentos

# COMMAND ----------

df = env.table(Table.extract_product_info_nao_medicamentos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["gpt_extract_product_info_nao_med"] = df
print(f"gpt_extract_product_info_nao_med: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### extract_product_info_nao_medicamentos_regex

# COMMAND ----------

df = env.table(Table.extract_product_info_nao_medicamentos_regex).filter(
    F.col("ean") == EAN
)
count = df.count()
if count > 0:
    tables_to_load["gpt_extract_product_info_nao_med_regex"] = df
print(f"gpt_extract_product_info_nao_med_regex: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### match_medicamentos

# COMMAND ----------

df = env.table(Table.match_medicamentos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["match_medicamentos"] = df
print(f"match_medicamentos: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### match_nao_medicamentos

# COMMAND ----------

df = env.table(Table.match_nao_medicamentos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["match_nao_medicamentos"] = df
print(f"match_nao_medicamentos: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### categorias_cascata_agregado

# COMMAND ----------

df = env.table(Table.categorias_cascata_agregado).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["categorias_cascata_agregado"] = df
print(f"categorias_cascata_agregado: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### GPT Tags Produtos

# COMMAND ----------

df = env.table(Table.enriquecimento_tags_produtos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["gpt_tags_produtos"] = df
print(f"gpt_tags_produtos: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Power Phrase Produtos

# COMMAND ----------

df = env.table(Table.power_phrase_produtos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["power_phrase_produtos"] = df
print(f"power_phrase_produtos: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### GPT Descrições Curtas Produtos

# COMMAND ----------

df = env.table(Table.gpt_descricoes_curtas_produtos).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["gpt_descricoes_curtas_produtos"] = df
print(f"gpt_descricoes_curtas_produtos: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### coluna_eh_medicamento_completo

# COMMAND ----------

df = env.table(Table.coluna_eh_medicamento_completo).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["coluna_eh_medicamento_completo"] = df
print(f"coluna_eh_medicamento_completo: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parsers de fontes externas (RD, CR, Farmarcas, etc.)

# COMMAND ----------

# Medicamentos ANVISA
# NOTE: Essa tabela não possui EAN, não tenho como fazer match
# df = env.table(Table.medicamentos_anvisa).filter(F.col("ean") == EAN)


# COMMAND ----------

# LMIP ANVISA
# NOTE: Essa tabela não possui EAN, não tenho como fazer match
# df = env.table(Table.lmip_anvisa).filter(F.col("ean") == EAN)


# COMMAND ----------

# MAGIC %md
# MAGIC #### IQVIA

# COMMAND ----------

iqvia_parser = IqviaParser.from_table(spark)
df = iqvia_parser.iqvia_df.filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["IQVIA_parser"] = df
print(f"IQVIA_parser: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### RD

# COMMAND ----------

rd_parser = RdParser.from_latest(spark, stage=stage)
df = rd_parser.produtos_raw.filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["RD_parser"] = df
print(f"RD_parser: {count} registro(s)")
rd_parser.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC #### CR

# COMMAND ----------

# NOTE: exclui o Consulta Remédios pois hoje ele não é usado no "agrega_enriquecimentos"
# try:
#     cr_path = RepositorioS3.ultima_pasta("1-raw-layer/maggu/consulta-remedios/", stage)
#     cr_path = f"s3://maggu-datalake-{stage}/{cr_path}*.json"
#     cr_rdd = spark.sparkContext.wholeTextFiles(cr_path)
#     cr_parser = CrParser(cr_rdd)
#     df = cr_parser.produtos_raw.filter(F.col("ean") == EAN)
#     count = df.count()
#     if count > 0:
#         tables_to_load["CR_parser"] = df
#         print(f"✅ CR_parser: {count} registro(s)")
#     cr_parser.produtos_raw.unpersist()
# except Exception as e:
#     print(f"⚠️ Erro ao carregar CR_parser: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Farmacas

# COMMAND ----------

farmarcas_parser = FarmarcasParser.from_sheets(spark)
df = farmarcas_parser.farmarcas_df.filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["Farmarcas_parser"] = df
print(f"Farmarcas_parser: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Minas Mais

# COMMAND ----------

minas_mais_parser = MinasMaisParser.from_s3_xlsx(spark)
df = minas_mais_parser.produtos_raw.filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["MinasMais_parser"] = df
print(f"MinasMais_parser: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANVISA

# COMMAND ----------

anvisa_parser = AnvisaParser.from_sheets(spark)
df = anvisa_parser.anvisa_df.filter(F.col("ean") == str(EAN))
count = df.count()
if count > 0:
    tables_to_load["ANVISA_parser"] = df
print(f"ANVISA_parser: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### BigDataCorp (produtos_raw)

# COMMAND ----------

df = env.table(Table.produtos_raw).filter(
    (F.col("ean") == EAN) & (F.col("fonte") == "BIGDATACORP")
)
count = df.count()
if count > 0:
    tables_to_load["BigDataCorp_raw"] = df
print(f"BigDataCorp_raw: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Websearch (marca e fabricante)

# COMMAND ----------

df = env.table(Table.enriquecimento_websearch).filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["Websearch_enriquecimento"] = df
print(f"Websearch_enriquecimento: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Correções Manuais (PostgreSQL)

# COMMAND ----------

df = env.postgres_adapter.read_table(spark, table="produtos_produtocorrecaomanual")
df = df.filter(F.col("ean") == EAN)
count = df.count()
if count > 0:
    tables_to_load["Correcoes_Manuais_Postgres"] = df
print(f"Correcoes_Manuais_Postgres: {count} registro(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extrair valores de colunas de cada fonte

# COMMAND ----------

print(f"\n📊 Total de tabelas carregadas: {len(tables_to_load)}")

# COMMAND ----------

comparison_data = []

for table_name, df in tables_to_load.items():
    # Obter colunas disponíveis na tabela
    available_cols = [c for c in COLUNAS_PARA_CHECAR if c in df.columns]

    if not available_cols:
        print(f"⚠️ Tabela '{table_name}' não possui nenhuma das colunas de interesse")
        continue

    # Coletar dados
    rows = df.select(available_cols).collect()

    for col_name in available_cols:
        # Extrair valores da coluna
        values = []
        for row in rows:
            val = getattr(row, col_name)
            # Converter para string legível
            if val is None:
                values.append(None)
            elif isinstance(val, list):
                # Manter arrays como JSON string para categorias
                values.append(json.dumps(val, ensure_ascii=False))
            else:
                values.append(str(val))

        # Remover None e duplicatas para verificar conflitos
        non_null_values = [v for v in values if v is not None]
        unique_values = list(set(non_null_values))

        # Determinar valor para display
        if len(non_null_values) == 0:
            display_value = None
        elif len(non_null_values) == 1:
            display_value = non_null_values[0]
        else:
            # Se houver múltiplos valores diferentes, concatenar
            if len(unique_values) == 1:
                display_value = non_null_values[0]
            else:
                display_value = " | ".join(unique_values[:3])  # Limitar a 3 valores

        # Para colunas de array, não calcular conflitos
        tem_conflitos = False
        if col_name not in COLUNAS_ARRAY_SEM_COMPARACAO:
            tem_conflitos = len(unique_values) > 1 if non_null_values else False

        comparison_data.append(
            {
                "coluna": col_name,
                "fonte": table_name,
                "valor": display_value,
                "tem_conflitos": tem_conflitos,
            }
        )

print(f"✅ Dados extraídos: {len(comparison_data)} entradas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar DataFrame de comparação

# COMMAND ----------

if not comparison_data:
    print("❌ Nenhum dado encontrado para o EAN especificado")
    dbutils.notebook.exit("Nenhum dado encontrado")

# COMMAND ----------

comparison_schema = T.StructType(
    [
        T.StructField("coluna", T.StringType(), False),
        T.StructField("fonte", T.StringType(), False),
        T.StructField("valor", T.StringType(), True),
        T.StructField("tem_conflitos", T.BooleanType(), False),
    ]
)

# COMMAND ----------

comparison_df = spark.createDataFrame(comparison_data, schema=comparison_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar tabela pivotada para visualização

# COMMAND ----------

# Obter lista de todas as fontes únicas
all_sources = [row.fonte for row in comparison_df.select("fonte").distinct().collect()]

# Pivotear: linhas = colunas, colunas = fontes
pivot_df = comparison_df.groupBy("coluna").pivot("fonte").agg(F.first("valor"))

# Ordenar colunas alfabeticamente para facilitar visualização
pivot_df = pivot_df.orderBy("coluna")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display da tabela de comparação

# COMMAND ----------

print(f"🔍 Comparação de fontes para o EAN: {EAN}")
print(f"📊 Colunas analisadas: {pivot_df.count()}")
print(f"📁 Fontes consultadas: {len(all_sources)}")

display(pivot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de conflitos entre fontes

# COMMAND ----------


def has_conflict(row):
    """Verifica se uma linha tem valores conflitantes entre fontes"""
    values = [
        row[col] for col in row.__fields__ if col != "coluna" and row[col] is not None
    ]
    unique_values = set(values)
    return len(unique_values) > 1


# COMMAND ----------

conflict_rows = []
for row in pivot_df.collect():
    # Ignorar colunas de array e colunas que não devem ser comparadas
    if row.coluna not in COLUNAS_ARRAY_SEM_COMPARACAO and has_conflict(row):
        conflict_rows.append(row.coluna)

# Essas colunas não são conflituosas nunca
if "atualizado_em" in conflict_rows:
    conflict_rows.remove("atualizado_em")
if "fonte" in conflict_rows:
    conflict_rows.remove("fonte")
if "gerado_em" in conflict_rows:
    conflict_rows.remove("gerado_em")


# COMMAND ----------

if conflict_rows:
    print(f"⚠️ Conflitos detectados em {len(conflict_rows)} coluna(s):")
    for col in conflict_rows:
        print(f"   - {col}")

    # Mostrar apenas as linhas com conflitos
    conflicts_df = pivot_df.filter(F.col("coluna").isin(conflict_rows))
    print("\n📋 Detalhes dos conflitos:")
    display(conflicts_df)

else:
    print("✅ Nenhum conflito detectado entre as fontes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo final

# COMMAND ----------

print("=" * 40)
print(f"RESUMO DA ANÁLISE - EAN: {EAN}")
print("=" * 40)
print(f"✅ Fontes consultadas: {len(tables_to_load)}")
print(f"✅ Colunas analisadas: {pivot_df.count()}")
print(f"⚠️ Colunas com conflitos: {len(conflict_rows) if conflict_rows else 0}")
print("=" * 40)

# TODO: seria interessante no futuro analisar tbm os produtos que tiverem com esse
# ean no vetor "eans_alternativos", pois o mescla_eans_similares pode acabar juntando as diferentes fontes
