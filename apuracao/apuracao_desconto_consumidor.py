# Databricks notebook source
# MAGIC %md
# MAGIC # Apuracao da campanha 'desconto consumidor'

# COMMAND ----------

# Este notebook apura o reembolso da campanha “Desconto do Consumidor” Culturelle, identificando vendas elegíveis, calculando o desconto aplicado e o valor a reembolsar por venda, conforme regras da campanha, para exportação em planilha.

# TODO: Por enquanto esta hardcodado para o Culturelle, mas pode ser adaptado para outras campanhas depois
# NOTE: voce vai perceber que eu hardcodei muitas coisas. Eu precisava fazer rapido!

# Regra de calculo:
# - O desconto máximo permitido é de 20% sobre o preço original do produto.
# - O preco original é calculado da seguinte forma (o que vier primeiro):
#   - 1. Valor da ultima venda do (ean, loja_id) antes da DATA_INICIO_CAMPANHA
#   - 2. Valor da primeira venda do (ean, loja_id) depois da DATA_INICIO_CAMPANHA
#   - 3. Valor de venda salvo na planilha de estoque no dia 6/jun (primeira data em que comecamos a salvar esses valores)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# NOTE: por enquanto so vai funcionar pra prod mesmo, ate aprender a usar o BQ em staging
STAGE = 'prod'
CATALOG = "production"

# COMMAND ----------

# Configuracoes gerais:

DESCONTO_MAX_PERMITIDO = 20  # Percentual máximo de desconto permitido
DATA_INICIO_CAMPANHA = "2025-05-20"  # 20 de maio de 2025
DATA_FIM_CAMPANHA = "2025-07-31"  # 31 de julho de 2025

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir eans e redes

# COMMAND ----------

# Definir EANs participantes
# NOTE: estou confiando que os Eans estao corretos e que nao falta nenhum EAN nessa lista, que veio do time de CS
#       Fiz um double check depois e nenhum desses produtos possui "eans_alternativos" no sistema, entao safe.
eans_list = [
    "7893454714745",  #  – Probiótico Culturelle Junior Com 10 Comprimidos
    "7893454714752",  #  – Culturelle Probiótico Júnior Caixa Com 30 Comprimidos Mastigáveis
    "7893454714776",  #  – Probiótico Culturelle Junior Com 6 Sachês De 1,2g
    "7893454714783",  #  – Probiótico Culturelle Junior Sem Açúcar Com 30 Sachês
    "7893454714806",  #  – Probiótico Culturelle Saúde Digestiva Com 10 Cápsulas
    "7893454714813",  #  – Probiótico Culturelle Saúde Digestiva Com 30 Cápsulas
    "7893454714905",  #  – Culturelle Probiot Maxx C/7 Ca
]


# COMMAND ----------

redes_nomes = [  # Coluna "databricks_tenant_name"
    "grupo_acacio",
    "farmacias_exclusivas",
    "farmacias_clinica",
    "farmacia_sao_francisco",
    "farmagui",
    "adifarma",
    "farmacia_anselmo",
    "farmacia_usimais",
]
# NOTE: esses ids sao de prod. Para staging seriam outros. Mas sem problemas
redes_ids = [
    "8318a8bf-e18a-4dfc-8056-eba410c945dc",  # grupo_acacio
    "47152cf9-0689-4c87-a90a-fef761b6057a",  # farmacias_exclusivas
    "d2ae60da-954c-48ed-b5b1-a70efa4e21b5",  # farmacias_clinica
    "6810c185-774f-49c3-a078-f8bc90e080a1",  # Farmácia São Francisco
    "b59d281a-8eed-4849-9b05-0f31f02c30ae",  # farmagui
    "ae5cac1d-1f11-48af-8177-796a2e04479a",  # adifarma
    "171e2683-86ef-4356-9c7c-0abf2e3b64dd",  # Farmácia Anselmo
    "80f2ba6d-21e2-4041-bbc7-72cd766fe6c7",  # Usimais
]

# TODO: o ideal seria ter definido isso como uma tuple, mas paciencia


# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar produtos

# COMMAND ----------

# NOTE: so carreguei produtos para puxar o nome dos produtos
produtos_refined = spark.read.table(f"{CATALOG}.refined.produtos_refined").cache()

# COMMAND ----------

# Filtrar produtos de interesse
produtos_refined_f = produtos_refined.filter(F.col("ean").isin(eans_list)).select(
    "ean",
    "eans_alternativos",
    "nome",
)

produtos_refined_f.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega dados de vendas

# COMMAND ----------

# Ler tabela de vendas (melhor pegar direto do bigquery, se disponivel)
# TODO: tem so pra prod isso daqui??? aprender a puxar de staging
# NOTE: eu puxei do BQ porque estamos tentando usar mais as tabelas de la.
vendas_venda_df = spark.read.table("bigquery.postgres_public.vendas_venda").cache()
vendas_item_df = spark.read.table("bigquery.postgres_public.vendas_item").cache()

vendas_item_df = vendas_item_df.filter(F.col("status") == "vendido")

# TODO: criar modo debug para controlar esses prints
# print(f"Quantidade de linhas em vendas_venda: {vendas_venda_df.count()}")
# print(f"Quantidade de linhas em vendas_item: {vendas_item_df.count()}")


# COMMAND ----------

# unir as tabelas de vendas e de itens
vendas_df = vendas_venda_df.join(
    vendas_item_df, vendas_venda_df.id == vendas_item_df.venda_id, "inner"
)

# print(f"Quantidade de linhas em vendas_df: {vendas_df.count()}")

# COMMAND ----------

# vendas_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agrega informacoes de conta

# COMMAND ----------

# precisa fazer o join com a tabela de contas para pegar o conta_id de cada loja_id
lojas_df = spark.read.table("bigquery.postgres_public.contas_loja").select(
    F.col("id").alias("loja_id"),
    F.col("name").alias("nome_loja"),
    "databricks_tenant_name",
    "codigo_loja",
    "conta_id",
)

vendas_df = vendas_df.join(
    lojas_df,
    on=vendas_df.loja_id == lojas_df.loja_id,
    how="left",
).drop(
    lojas_df.loja_id,
)

# print(f"Quantidade de linhas em vendas_df: {vendas_df.count()}")

# COMMAND ----------

# rows = vendas_df.select('loja_id').distinct().collect()
# lojas_ids = [row.loja_id for row in rows]
# print(len(lojas_ids))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Filtrar vendas elegíveis (EANs, redes, período)
# MAGIC

# COMMAND ----------


vendas_filtradas_df = vendas_df.filter(
    (F.col("ean").isin(eans_list))
    & (F.col("conta_id").isin(redes_ids))
    & (F.col("realizada_em") >= F.lit(DATA_INICIO_CAMPANHA))
    & (F.col("realizada_em") <= F.lit(DATA_FIM_CAMPANHA))
).cache()

print(f"Quantidade de linhas em vendas_filtradas_df: {vendas_filtradas_df.count()}")
# Essas sao as vendas elegiveis para receber cashback...


# COMMAND ----------

# E1: Buscar vendas imediatamente antes da campanha

vendas_antes_campanha = vendas_df.filter(
    (F.col("realizada_em") < F.lit(DATA_INICIO_CAMPANHA))
    & (F.col("ean").isin(eans_list))
    & (F.col("conta_id").isin(redes_ids))
)
# vendas_antes_campanha_count = vendas_antes_campanha.count()
# print(f"Quantidade de vendas antes da campanha: {vendas_antes_campanha_count}")

# if vendas_antes_campanha_count == 0:
#     print(
#         f"ALERTA: Não há vendas registradas antes da data de início da campanha ({DATA_INICIO_CAMPANHA}). "
#         "Verifique os dados de entrada."
#     )
# else:
#     vendas_antes_campanha.limit(10).display()


# COMMAND ----------

# O 3650 é um numero suficientemente maior que o maximo de dias entre duas datas
window_loja_ean_data = F.window("data_venda", "3650 days")
precos_anteriores = (
    vendas_df.filter(F.col("realizada_em") < F.lit(DATA_INICIO_CAMPANHA))
    .groupBy("conta_id", "loja_id", "ean")
    .agg(F.max("realizada_em").alias("data_ultima_venda_antes"))
)

# q: o que sera que a gente vai fazer com os casos que nao possuem uma venda antes dessa data?
# a: a partir do dia 6/junho temos os valores de preco (valor_venda_desconto) no delta!

precos_anteriores = precos_anteriores.join(
    vendas_df.select("conta_id", "loja_id", "ean", "realizada_em", "valor_final"),
    (precos_anteriores.conta_id == vendas_df.conta_id)
    & (precos_anteriores.loja_id == vendas_df.loja_id)
    & (precos_anteriores.ean == vendas_df.ean)
    & (precos_anteriores.data_ultima_venda_antes == vendas_df.realizada_em),
    how="left",
).select(
    precos_anteriores.conta_id,
    precos_anteriores.loja_id,
    precos_anteriores.ean,
    F.col("valor_final").alias("preco_antes_inicio_campanha"),
    F.col("realizada_em").alias("data_ultima_venda_antes_inicio_campanha"),
)

precos_anteriores.limit(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## E2: Primeira redução de preço após DATA_INICIO_CAMPANHA (por loja/EAN)

# COMMAND ----------

vendas_pos = vendas_filtradas_df.filter(
    F.col("realizada_em") >= F.lit(DATA_INICIO_CAMPANHA)
)
window_ordem = Window.partitionBy("conta_id", "loja_id", "ean").orderBy("realizada_em")
vendas_pos = vendas_pos.withColumn(
    "preco_anterior", F.lag("valor_final").over(window_ordem)
)
vendas_pos = vendas_pos.withColumn(
    "houve_reducao",
    (F.col("preco_anterior").isNotNull())
    & (F.col("valor_final") < F.col("preco_anterior")),
)
primeira_reducao = (
    vendas_pos.filter(F.col("houve_reducao"))
    .groupBy("conta_id", "loja_id", "ean")
    .agg(F.min("realizada_em").alias("data_primeira_reducao"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Extra: consultar tabela de estoque historico

# COMMAND ----------

# Ler df de estoque historico
estoque_historico = spark.read.table(
    f"{CATALOG}.analytics.estoque_historico_eans_campanha_e_concorrentes"
).cache()

# testei com estoque pelo s3 em vez de delta, mas no delta parecia mais completa a informacao
# TABELA_S3_FOLDER = f"s3://maggu-datalake-{STAGE}/5-sharing-layer/copilot.maggu.ai/estoque_historico_formatado/"
# estoque_historico = spark.read.format("parquet").load(TABELA_S3_FOLDER).cache()

estoque_historico.printSchema()

# NOTE: informacao de preco nao esta disponivel no s3, enquanto que no delta a coluna esta vazia para datas antes de 3 de junho.


# COMMAND ----------

# Filtrar EANs
estoque_historico_f = estoque_historico.filter(F.col("ean").isin(eans_list))

# remover linhas onde a coluna "ean" ou "codigo_loja" estao nulos

# Ordenar por data_extracao
estoque_historico_f = estoque_historico_f.orderBy("data_extracao", ascending=True)

# Remover duplicatas em (loja_id, ean)  # Obs.: assim fica so o dado mais antigo de cada loja
estoque_historico_f = estoque_historico_f.dropDuplicates(["loja_id", "ean"])

# Filtrar data
# estoque_historico_f = estoque_historico.filter(
#     F.col("data_extracao").cast("date") == "2025-06-03"
# )

# Filtrar redes (ou loja)
# estoque_historico_f = estoque_historico_f.filter(
#     F.col("loja_id").isin(lojas_ids)
# )  # NOTE: nao funcionou pq a coluna loja_id ainda nao tava populada nessa epoca. Vou fazer por tenant

estoque_historico_f = estoque_historico_f.filter(F.col("tenant").isin(redes_nomes))


# COMMAND ----------

estoque_historico_f.filter(F.col("loja_id").isNull()).select(
    "tenant", "codigo_loja", "loja_id"
).distinct().display()


# COMMAND ----------

estoque_historico_f = (
    estoque_historico_f.drop("loja_id")
    .alias("estoque")
    .join(
        lojas_df.withColumnRenamed("databricks_tenant_name", "tenant"),
        on=[
            "tenant",
            "codigo_loja",
        ],
        how="left",
    )
    .select("estoque.*", F.col("loja_id"))
)

# Display teste
estoque_historico_f.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Juntar tudo e calcular colunas finais

# COMMAND ----------


vendas_final = vendas_filtradas_df.join(
    precos_anteriores, on=["conta_id", "loja_id", "ean"], how="left"
).join(primeira_reducao, on=["conta_id", "loja_id", "ean"], how="left")


# COMMAND ----------

# 1. Buscar valor da primeira venda após o início da campanha (caso não exista venda anterior)
vendas_primeira_pos = (
    vendas_df.filter(
        (F.col("realizada_em") >= F.lit(DATA_INICIO_CAMPANHA))
        & (F.col("ean").isin(eans_list))
        & (F.col("conta_id").isin(redes_ids))
    )
    .withColumn(
        "row_number",
        F.row_number().over(
            Window.partitionBy("conta_id", "loja_id", "ean").orderBy("realizada_em")
        ),
    )
    .filter(F.col("row_number") == 1)
)

precos_primeira_pos = vendas_primeira_pos.select(
    "conta_id",
    "loja_id",
    "ean",
    "valor_final",
    "realizada_em",
)
precos_primeira_pos = precos_primeira_pos.withColumnRenamed(
    "valor_final", "preco_primeira_venda_pos"
).withColumnRenamed("realizada_em", "data_primeira_venda_pos")


# COMMAND ----------

# 2. Buscar valor do estoque histórico do dia 6/jun (primeira data disponível)
estoque_historico_f = estoque_historico_f.withColumnRenamed(
    "preco_venda_desconto", "preco_estoque_historico"
).select("loja_id", "ean", "preco_estoque_historico", "data_extracao")

# COMMAND ----------

# 3. Juntar as opções de preço original
vendas_final = vendas_final.join(
    precos_primeira_pos, ["conta_id", "loja_id", "ean"], how="left"
).join(estoque_historico_f, ["loja_id", "ean"], how="left")

# 4. Definir preco_original seguindo a ordem de prioridade
vendas_final = vendas_final.withColumn(
    "preco_original",
    F.coalesce(
        F.col("preco_antes_inicio_campanha"),
        F.col("preco_primeira_venda_pos"),
        F.col("preco_estoque_historico"),
    ),
)

# 4b. Salvar a fonte e a data do preco_original
vendas_final = vendas_final.withColumn(
    "fonte_preco_original",
    F.when(F.col("preco_antes_inicio_campanha").isNotNull(), F.lit("venda_antes"))
    .when(F.col("preco_primeira_venda_pos").isNotNull(), F.lit("venda_depois"))
    .when(F.col("preco_estoque_historico").isNotNull(), F.lit("estoque_historico"))
    .otherwise(F.lit(None)),
)
vendas_final = vendas_final.withColumn(
    "data_preco_original",
    F.when(
        F.col("fonte_preco_original") == "venda_antes",
        F.col("data_ultima_venda_antes_inicio_campanha"),
    )
    .when(
        F.col("fonte_preco_original") == "venda_depois",
        F.col("data_primeira_venda_pos"),
    )
    .when(F.col("fonte_preco_original") == "estoque_historico", F.col("data_extracao"))
    .otherwise(F.lit(None)),
)

# 5. Calcular % desconto e valor a reembolsar usando preco_original
vendas_final = (
    vendas_final.withColumn(
        "valor_venda_unitario",
        F.round(F.col("valor_final") / F.col("quantidade"), 2),
    )
    .withColumn(
        "pct_desconto",
        F.round(
            (F.col("preco_original") - F.col("valor_venda_unitario"))
            / F.col("preco_original")
            * 100,
            2,
        ),
    )
    .withColumn(
        "pct_desconto_limitado",
        F.when(
            F.col("pct_desconto") > DESCONTO_MAX_PERMITIDO, DESCONTO_MAX_PERMITIDO
        ).otherwise(F.col("pct_desconto")),
    )
    .withColumn(
        "valor_reembolso_unitario",
        F.round(
            F.col("preco_original") * (F.col("pct_desconto_limitado") / 100),
            2,
        ),
    )
    .withColumn(
        "valor_reembolso_total",
        F.greatest(
            F.round(F.col("valor_reembolso_unitario") * F.col("quantidade"), 2),
            F.lit(0),
        ),
    )
)

# COMMAND ----------

# Agregar nome do produto
vendas_final = vendas_final.join(
    produtos_refined_f.select("ean", "nome").alias("produtos_refined"),
    on="ean",
    how="left",
).withColumnRenamed("nome", "nome_produto")

# COMMAND ----------

# incluir uma coluna com a data inicio campanha (da igual que seja o mesmo valor para todas)
vendas_final = vendas_final.withColumn(
    "data_inicio_campanha", F.lit(DATA_INICIO_CAMPANHA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Selecionar colunas finais para exportação

# COMMAND ----------


colunas_finais = [
    "realizada_em",
    "databricks_tenant_name",
    "nome_loja",
    "produtos_refined.ean",
    "nome_produto",
    "data_ultima_venda_antes_inicio_campanha",
    "data_inicio_campanha",
    "data_primeira_reducao",
    "quantidade",
    "valor_venda_unitario",
    "valor_final",
    "preco_antes_inicio_campanha",
    "preco_original",
    "fonte_preco_original",
    "data_preco_original",
    "pct_desconto",
    "pct_desconto_limitado",
    "valor_reembolso_unitario",
    "valor_reembolso_total",
]
df_resultado = vendas_final.select(*colunas_finais)


# COMMAND ----------

# Renomear colunas para ficar mais legivel pro time de CS
df_resultado = (
    df_resultado.withColumnRenamed("realizada_em", "venda_realizada_em")
    .withColumnRenamed("databricks_tenant_name", "nome_rede")
    .withColumnRenamed("valor_final", "valor_venda_total")
)

# COMMAND ----------

# converter colunas timestamp para data
formato_data = "yyyy-MM-dd"
df_resultado = (
    df_resultado.withColumn(
        "venda_realizada_em", F.date_format(F.col("venda_realizada_em"), formato_data)
    )
    .withColumn(
        "data_primeira_reducao",
        F.date_format(F.col("data_primeira_reducao"), formato_data),
    )
    .withColumn(
        "data_ultima_venda_antes_inicio_campanha",
        F.date_format(F.col("data_ultima_venda_antes_inicio_campanha"), formato_data),
    )
    .withColumn(
        "data_inicio_campanha",
        F.date_format(F.col("data_inicio_campanha"), formato_data),
    )
)


# COMMAND ----------

# NOTE: A partir do display abaixo eu exportei pra um csv, dai fiz o upload no drive e colori a planilha.
display(df_resultado)

# TODO: salvar em uma view do databricks camada analytics
