# Databricks notebook source
# MAGIC %md
# MAGIC # Cria `clientes_conta_refined`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])

# COMMAND ----------

from datetime import date

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.context import SparkConf
from pyspark.sql import SparkSession, Window

from maggulake.clientes.calcular_rfm import calculate_rfm
from maggulake.enums import TamanhoProduto, TipoMedicamento
from maggulake.schemas import schema_clientes_conta_refined
from maggulake.utils.calcula_coluna_maior_valor import calcula_coluna_maior_valor
from maggulake.utils.numbers import calcula_idade, map_faixa_etaria
from maggulake.utils.pyspark import to_schema

# COMMAND ----------

config = SparkConf().setAll(pairs=[('spark.sql.caseSensitive', 'true')])

spark = (
    SparkSession.builder.appName('Postgres Client').config(conf=config).getOrCreate()
)

# por enquanto só refinamos em prod, pois não tem vendas em staging
# STAGE = dbutils.widgets.get("stage")
STAGE = "prod"
CATALOG = "production" if STAGE == "prod" else "staging"
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

# Delta Lake
tabela_clientes_conta = f"{CATALOG}.standard.clientes_conta_standard"
tabela_vendas = f"{CATALOG}.analytics.view_vendas"
tabela_cestas = f"{CATALOG}.analytics.view_cestas_de_compras"
tabela_produtos_refined = f"{CATALOG}.refined.produtos_refined"
DELTA_TABLE_REFINED = f"{CATALOG}.refined.clientes_conta_refined"
S3_REFINED_PATH = f"s3://maggu-datalake-{STAGE}/3-refined-layer/clientes_conta"

# S3
s3_cpfs_file_path = "s3://maggu-datalake-prod/3-refined-layer/cpfs_processados"
# TODO: migrar a tabela abaixo para um delta table
s3_clientes_tags_folder = "s3://maggu-datalake-prod/3-refined-layer/gpt_tags_clientes"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importa tabela standard

# COMMAND ----------

df = spark.read.table(tabela_clientes_conta).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carrega e prepara tabela standard

# COMMAND ----------

calcula_idade_udf = F.udf(calcula_idade, T.IntegerType())
map_faixa_etaria_udf = F.udf(map_faixa_etaria)

# COMMAND ----------

# Remove traços e pontos da coluna `cpf_cnpj`
df = df.withColumn("cpf_cnpj", F.regexp_replace("cpf_cnpj", "[^0-9]", ""))

# COMMAND ----------

# NOTE: Todo CPF tem 11 dígitos, todo CNPJ tem 14 dígitos
CPF_LENGTH = 11
CNPJ_LENGTH = 14

df = df.withColumn(
    "eh_pessoa_fisica",
    F.when(F.length("cpf_cnpj") == CPF_LENGTH, True)
    .when(F.length("cpf_cnpj") == CNPJ_LENGTH, False)
    .cast("boolean"),
)

df = df.withColumn(
    "eh_pessoa_juridica",
    F.when(F.length("cpf_cnpj") == CPF_LENGTH, False)
    .when(F.length("cpf_cnpj") == CNPJ_LENGTH, True)
    .cast("boolean"),
)

# COMMAND ----------

# tratamento das colunas data_nascimento (DATE), idade(INT) e faixa_etaria (STRING)
df = df.withColumn("idade", calcula_idade_udf(F.col("data_nascimento")))
df = df.withColumn("faixa_etaria", map_faixa_etaria_udf(F.col("idade")))

# COMMAND ----------

# criar chave concatenada
df = df.withColumn("concatenado", F.concat(df.conta, df.cpf_cnpj))

# COMMAND ----------

# Teste
df.limit(100).display()

# COMMAND ----------

# Visualizar quantidade de clientes de cada conta
# df.groupBy("conta").count().orderBy("count", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enriquece com informacoes bigboost

# COMMAND ----------

bigboost_df = spark.read.parquet(s3_cpfs_file_path).cache()
bigboost_df = bigboost_df.dropDuplicates(["cpf_cnpj"])

bigboost_df = bigboost_df.withColumnRenamed("cpf_cnpj", "cpf_cnpj_bb")


# COMMAND ----------

df = df.join(bigboost_df, df.cpf_cnpj == bigboost_df.cpf_cnpj_bb, how="left")

# Preenche com infos do bigboost
df = (
    df.withColumn("cpf_cnpj", F.coalesce("cpf_cnpj", "cpf_cnpj_bb"))
    .withColumn("nome_completo", F.coalesce("nome_completo", "nome_completo_bb"))
    .withColumn("sexo", F.coalesce("sexo", "sexo_bb"))
    .withColumn("data_nascimento", F.coalesce("data_nascimento", "data_nascimento_bb"))
)

# drop colunas bb
df = df.drop("cpf_cnpj_bb", "nome_completo_bb", "sexo_bb", "data_nascimento_bb")

# Compliance de sexo
df = df.withColumn(
    "sexo",
    F.when(F.col("sexo") == "nao-informado", None).otherwise(F.col("sexo")),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecimentos com vendas

# COMMAND ----------

# Importar tabela de vendas
vendas_df = spark.read.table(tabela_vendas).cache()

# COMMAND ----------

# Remover linhas em que a coluna `cpf_cnpj_cliente` está vazia
vendas_df = vendas_df.filter(vendas_df.cpf_cnpj_cliente.isNotNull())

# Remover pontos e traços da coluna cpf_cnpj_cliente
vendas_df = vendas_df.withColumn(
    "cpf_cnpj_cliente", F.regexp_replace("cpf_cnpj_cliente", "[^0-9]", "")
)

# COMMAND ----------

# criar uma chave concatenada
vendas_df = vendas_df.withColumn(
    "concatenado", F.concat(vendas_df.conta_loja, vendas_df.cpf_cnpj_cliente)
)

# Tempos preferidos de compra:
vendas_df = vendas_df.withColumn(
    "dia_da_semana",
    F.dayofweek(vendas_df.realizada_em),
)
vendas_df = vendas_df.withColumn(
    "dia_do_mes",
    F.dayofmonth(vendas_df.realizada_em),
)
vendas_df = vendas_df.withColumn(
    "hora_do_dia",
    F.hour(vendas_df.realizada_em),
)

# trocar coluna 'eh_medicamento_produto' para inteiro: se for True, colocar 1
vendas_df = vendas_df.withColumn(
    "eh_medicamento_produto",
    F.when(vendas_df.eh_medicamento_produto == True, 1).otherwise(0),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculo RFM

# COMMAND ----------

hoje = date.today()
data_atual = F.lit(hoje)

# RFM:
vendas_rfm_df = vendas_df.groupBy("concatenado").agg(
    F.min("realizada_em").alias("data_primeira_compra"),
    F.max("realizada_em").alias("data_ultima_compra"),  # Recência
    F.countDistinct("venda_id").alias("total_de_compras"),  # Frequência
    F.countDistinct("pre_venda_id").alias("total_de_cestas"),
    F.countDistinct("venda_item_id").alias("total_de_itens"),
    F.collect_list("venda_id").alias("compras"),
    F.sum("preco_venda_desconto").alias("valor_total"),  # Valor Monetário
    F.mode("codigo_loja").alias("loja_mais_frequente"),
    F.mode("dia_do_mes").alias("pref_dia_mes"),
    F.mode("hora_do_dia").alias("pref_hora"),
    F.mode("dia_da_semana").alias("pref_dia_semana"),
    F.sum("eh_medicamento_produto").alias("eh_medicamento_produto"),
)

# COMMAND ----------

# ja_comprou_medicamento
vendas_rfm_df = vendas_rfm_df.withColumn(
    "ja_comprou_medicamento",
    F.when(F.col("eh_medicamento_produto") > 0, True).otherwise(False),
)

# ticket_medio
vendas_rfm_df = vendas_rfm_df.withColumn(
    "ticket_medio",
    F.round((vendas_rfm_df.valor_total / vendas_rfm_df.total_de_compras), 2),
)

# COMMAND ----------

# converter timestamp para date
vendas_rfm_df = vendas_rfm_df.withColumn(
    "data_ultima_compra",
    F.to_date(vendas_rfm_df.data_ultima_compra),
)
vendas_rfm_df = vendas_rfm_df.withColumn(
    "data_primeira_compra",
    F.to_date(vendas_rfm_df.data_primeira_compra),
)


# 'intervalo_medio_entre_compras'
vendas_rfm_df = vendas_rfm_df.withColumn(
    "intervalo_medio_entre_compras",
    F.datediff(vendas_rfm_df.data_ultima_compra, vendas_rfm_df.data_primeira_compra)
    / vendas_rfm_df.total_de_compras,
).withColumn(
    "intervalo_medio_entre_compras", F.col("intervalo_medio_entre_compras").cast("int")
)


# 'media_itens_por_cesta'
vendas_rfm_df = vendas_rfm_df.withColumn(
    "media_itens_por_cesta",
    F.round(
        (vendas_rfm_df.total_de_itens / vendas_rfm_df.total_de_compras).cast("float"), 2
    ),
)

# COMMAND ----------

# Calcular o cluster RFM em que cada cliente se encaixa.
vendas_rfm_df = calculate_rfm(
    data=vendas_rfm_df,
    recency_col="data_ultima_compra",
    frequency_col="total_de_compras",
    monetary_col="ticket_medio",
)
vendas_rfm_df = vendas_rfm_df.withColumnRenamed("RFV", "cluster_rfm")

# COMMAND ----------

# Porcentagem de itens que são medicamentos (evitar divisão por zero)
vendas_rfm_df = vendas_rfm_df.withColumn(
    "pct_itens_medicamentos",
    F.when(
        F.col("total_de_itens") != 0,
        (F.col("eh_medicamento_produto") / F.col("total_de_itens")) * 100,
    )
    .otherwise(F.lit(None))
    .cast("float"),
)

# COMMAND ----------

# vendas_rfm_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### join entre df e vendas_rfm_df

# COMMAND ----------

clientes_rfm_joined_df = df.join(vendas_rfm_df, on="concatenado", how="left")
# clientes_rfm_joined_df.display()

# COMMAND ----------

# vendas_df.count()
# antes de remover clientes sem CPF: 1526921
# depois: 668125
# diferença: 858796 (56% das vendas não são identificadas)

# COMMAND ----------

# vendas_df.display()
# vendas_df.select("conta_loja").distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lista de medicamentos tarjados e controlados

# COMMAND ----------

# no vendas_df tem uma coluna "eh_tarjado_produto" e uma coluna "eh_controlado_produto", que são booleanas
# quando estas colunas forem True em cada caso, pegar o valor da coluna "nome_produto"

vendas_df = vendas_df.withColumn(
    "nome_produto_tarjado",
    F.when(
        F.col("eh_tarjado_produto") == True,
        F.split(F.col("nome_produto"), ",")[0],
    ).otherwise(F.lit(None)),
)

vendas_df = vendas_df.withColumn(
    "nome_produto_controlado",
    F.when(
        F.col("eh_controlado_produto") == True,
        F.split(F.col("nome_produto"), ",")[0],
    ).otherwise(F.lit(None)),
)

vendas_df_gp = vendas_df.groupBy("concatenado").agg(
    F.collect_list("nome_produto_tarjado").alias("medicamentos_tarjados"),
    F.collect_list("nome_produto_controlado").alias("medicamentos_controle"),
)

clientes_rfm_joined_df = clientes_rfm_joined_df.join(
    vendas_df_gp, on=["concatenado"], how="left"
)

clientes_rfm_joined_df = clientes_rfm_joined_df.withColumn(
    "medicamentos_tarjados", F.array_distinct(F.col("medicamentos_tarjados"))
).withColumn("medicamentos_controle", F.array_distinct(F.col("medicamentos_controle")))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Tamanho de produtos preferidos

# COMMAND ----------

# Calcula porcentagem de produtos de cada tamanho (Pequeno, Medio, Grande)
# TODO: Verificar o preenchimento da coluna tamanho_produto na table produtos_v2
vendas_df = (
    vendas_df.withColumn(
        "tamanho_pequeno",
        F.when(F.col("tamanho_produto") == TamanhoProduto.PEQUENO.value, 1).otherwise(
            0
        ),
    )
    .withColumn(
        "tamanho_medio",
        F.when(F.col("tamanho_produto") == TamanhoProduto.MEDIO.value, 1).otherwise(0),
    )
    .withColumn(
        "tamanho_grande",
        F.when(F.col("tamanho_produto") == TamanhoProduto.GRANDE.value, 1).otherwise(0),
    )
)

vendas_df_gp = vendas_df.groupBy("concatenado").agg(
    F.sum("tamanho_pequeno").alias("total_pequeno"),
    F.sum("tamanho_medio").alias("total_medio"),
    F.sum("tamanho_grande").alias("total_grande"),
    F.count("tamanho_produto").alias("total_produtos"),
)

vendas_df_gp = (
    vendas_df_gp.withColumn(
        "pct_pequeno", F.col("total_pequeno") / F.col("total_produtos")
    )
    .withColumn("pct_medio", F.col("total_medio") / F.col("total_produtos"))
    .withColumn("pct_grande", F.col("total_grande") / F.col("total_produtos"))
)

clientes_rfm_joined_df = clientes_rfm_joined_df.join(
    vendas_df_gp, on=["concatenado"], how="left"
)

clientes_rfm_joined_df = clientes_rfm_joined_df.withColumn(
    "tamanho_produto_preferido",
    F.when(
        (F.col("pct_pequeno") >= F.col("pct_medio"))
        & (F.col("pct_pequeno") >= F.col("pct_grande")),
        TamanhoProduto.PEQUENO.value,
    )
    .when(
        (F.col("pct_medio") >= F.col("pct_pequeno"))
        & (F.col("pct_medio") >= F.col("pct_grande")),
        TamanhoProduto.MEDIO.value,
    )
    .when(
        (F.col("pct_grande") >= F.col("pct_pequeno"))
        & (F.col("pct_grande") >= F.col("pct_medio")),
        TamanhoProduto.GRANDE.value,
    )
    .otherwise(F.lit(None)),
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Forma farmaceutica preferida(s)

# COMMAND ----------

# agrupar por cpf_cnpj e por forma_farmaceutica_produto, contando a quantidade de cada valor unico de forma_farmaceutica_produto para cada cpf_cnpj
forma_farmaceutica_df = vendas_df.groupBy(
    "cpf_cnpj_cliente", "forma_farmaceutica_produto"
).agg(F.count("forma_farmaceutica_produto").alias("qtd_formas_farmaceuticas"))

# para cada CPF, verificar qual o maior valor de qtd_formas_farmaceuticas, escolher o valor da coluna "forma_farmaceutica_produto" correspondente e atribuir a coluna "forma_farmaceutica_preferida"

forma_farmaceutica_df = forma_farmaceutica_df.withColumn(
    "max_qtd_formas_farmaceuticas",
    F.max("qtd_formas_farmaceuticas").over(Window.partitionBy("cpf_cnpj_cliente")),
)

forma_farmaceutica_df = forma_farmaceutica_df.withColumn(
    "forma_farmaceutica_preferida",
    F.when(
        F.col("qtd_formas_farmaceuticas") == F.col("max_qtd_formas_farmaceuticas"),
        F.col("forma_farmaceutica_produto"),
    ).otherwise(F.lit(None)),
)

forma_farmaceutica_df = forma_farmaceutica_df.select(
    "cpf_cnpj_cliente",
    "forma_farmaceutica_preferida",
)

forma_farmaceutica_df = forma_farmaceutica_df.filter(
    F.col("forma_farmaceutica_preferida").isNotNull()
)

forma_farmaceutica_df = forma_farmaceutica_df.dropDuplicates()

# transformar para uma lista quando a moda nao eh unica
forma_farmaceutica_df = forma_farmaceutica_df.groupBy("cpf_cnpj_cliente").agg(
    F.collect_list("forma_farmaceutica_preferida").alias(
        "formas_farmaceuticas_preferidas"
    )
)

forma_farmaceutica_df = forma_farmaceutica_df.withColumnRenamed(
    "cpf_cnpj_cliente", "cpf_cnpj"
)

# forma_farmaceutica_df.display()


# COMMAND ----------

clientes_rfm_joined_df = clientes_rfm_joined_df.join(
    forma_farmaceutica_df, on=["cpf_cnpj"], how="left"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecimento com cestas

# COMMAND ----------

# Seleciona a informacao de tipo_medicamento para o enriquecimento
df_check_generico = (
    spark.read.table(tabela_produtos_refined)
    .select("id", 'tipo_medicamento')
    .withColumnRenamed('id', 'product_id')
)

# COMMAND ----------

# ler tabela de cestas
cestas_df = spark.read.table(tabela_cestas).cache()

# enriquece com a coluna tipo_medicamento
cestas_df = cestas_df.join(df_check_generico, on=["product_id"], how="left")

cestas_df = cestas_df.select(
    "basket_id",
    "product_id",
    "store_id",
    "customer_id",
    "product_ean",
    "sale_channel",
    "payment_method",
    "tipo_venda",
    "tipo_comissao",
    "customer_cpf",
    "symptoms",
    "tipo_medicamento",
)
# criar chave concatenada
cestas_df = cestas_df.withColumn(
    "concatenado", F.concat(F.col("customer_id"), F.lit("_"), F.col("store_id"))
)

# COMMAND ----------

# Valida se o medicamento é generico, referencia, similar ou similar intercambiavel


cestas_df = cestas_df.withColumn(
    "qtde_generico",
    F.when(F.col("tipo_medicamento") == TipoMedicamento.GENERICO.value, 1).otherwise(0),
)

cestas_df = cestas_df.withColumn(
    "qtde_referencia",
    F.when(F.col("tipo_medicamento") == TipoMedicamento.REFERENCIA.value, 1).otherwise(
        0
    ),
)

cestas_df = cestas_df.withColumn(
    "qtde_similar",
    F.when(F.col("tipo_medicamento") == TipoMedicamento.SIMILAR.value, 1).otherwise(0),
)

cestas_df = cestas_df.withColumn(
    "qtde_similar_intercambiavel",
    F.when(
        F.col("tipo_medicamento") == TipoMedicamento.SIMILAR_INTERCAMBIAVEL.value, 1
    ).otherwise(0),
)

# COMMAND ----------

# Formato de venda/entrega

cestas_df = cestas_df.withColumn(
    "qtde_balcao", F.when(F.col("sale_channel") == "balcao", 1).otherwise(0)
)
cestas_df = cestas_df.withColumn(
    "qtde_entrega",
    F.when(
        ((F.col("sale_channel") == "entrega") | (F.col("sale_channel") == "retirada")),
        1,
    ).otherwise(0),
)

# COMMAND ----------

# Forma de pagamento

cestas_df = cestas_df.withColumn(
    "qtde_pix", F.when(F.col("payment_method") == "pix", 1).otherwise(0)
)
cestas_df = cestas_df.withColumn(
    "qtde_dinheiro", F.when(F.col("payment_method") == "dinheiro", 1).otherwise(0)
)
cestas_df = cestas_df.withColumn(
    "qtde_cartao", F.when(F.col("payment_method") == "cartao", 1).otherwise(0)
)
cestas_df = cestas_df.withColumn(
    "qtde_crediario", F.when(F.col("payment_method") == "crediario", 1).otherwise(0)
)

# cestas_df.display()

# COMMAND ----------

# Vendas agregadas, substituicao, organica

cestas_df = cestas_df.withColumn(
    "qtde_venda_agregada", F.when(F.col("tipo_venda") == "agregada", 1).otherwise(0)
)

cestas_df = cestas_df.withColumn(
    "qtde_venda_substituicao",
    F.when(F.col("tipo_venda") == "substituicao", 1).otherwise(0),
)

cestas_df = cestas_df.withColumn(
    "qtde_venda_organica", F.when(F.col("tipo_venda") == "organica", 1).otherwise(0)
)

# COMMAND ----------

# groupBy concatenado
cestas_df_grouped = cestas_df.groupBy("concatenado").agg(
    F.sum("qtde_balcao").alias("qtde_balcao"),
    F.sum("qtde_entrega").alias("qtde_entrega"),
    F.sum("qtde_pix").alias("qtde_pix"),
    F.sum("qtde_dinheiro").alias("qtde_dinheiro"),
    F.sum("qtde_cartao").alias("qtde_cartao"),
    F.sum("qtde_crediario").alias("qtde_crediario"),
    F.count("concatenado").alias("qtde_vendas"),
    F.first("customer_id").alias("customer_id"),
    F.first("store_id").alias("store_id"),
    F.first("customer_cpf").alias("customer_cpf"),
    F.sum('qtde_generico').alias("qtde_generico"),
    F.sum('qtde_referencia').alias("qtde_referencia"),
    F.sum('qtde_similar').alias("qtde_similar"),
    F.sum('qtde_similar_intercambiavel').alias("qtde_similar_intercambiavel"),
    F.sum('qtde_venda_agregada').alias("qtde_venda_agregada"),
    F.sum('qtde_venda_substituicao').alias("qtde_venda_substituicao"),
    F.sum('qtde_venda_organica').alias("qtde_venda_organica"),
)
# drop onde concatenado for null
cestas_df_grouped = cestas_df_grouped.filter(F.col("concatenado").isNotNull())

# COMMAND ----------

# Porcentagem medicamentos genericos,referencia,similar,similar intercambiavel
# TODO: nao dividir pelas vendas totais, mas sim pelas vendas de medicamentos
cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_itens_medicamentos_genericos', F.col("qtde_generico") / F.col("qtde_vendas")
)

cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_itens_medicamentos_referencia', F.col("qtde_referencia") / F.col("qtde_vendas")
)

cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_itens_medicamentos_similar', F.col("qtde_similar") / F.col("qtde_vendas")
)

cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_itens_medicamentos_similar_intercambiavel',
    F.col("qtde_similar_intercambiavel") / F.col("qtde_vendas"),
)

# COMMAND ----------

# Porcentagem de vendas agregadas,organica,substituicao
cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_venda_agregada', F.col("qtde_venda_agregada") / F.col("qtde_vendas")
)

cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_venda_organica', F.col("qtde_venda_organica") / F.col("qtde_vendas")
)

cestas_df_grouped = cestas_df_grouped.withColumn(
    'pct_venda_substituicao', F.col("qtde_venda_substituicao") / F.col("qtde_vendas")
)

# COMMAND ----------

# Porcentagens de meios de pagamento:
cestas_df_grouped = cestas_df_grouped.withColumn(
    "pct_pgto_pix", F.col("qtde_pix") / F.col("qtde_vendas")
)
cestas_df_grouped = cestas_df_grouped.withColumn(
    "pct_pgto_cartao", F.col("qtde_cartao") / F.col("qtde_vendas")
)
cestas_df_grouped = cestas_df_grouped.withColumn(
    "pct_pgto_crediario", F.col("qtde_crediario") / F.col("qtde_vendas")
)
cestas_df_grouped = cestas_df_grouped.withColumn(
    "pct_pgto_dinheiro", F.col("qtde_dinheiro") / F.col("qtde_vendas")
)

# COMMAND ----------

# Porcentagens de entregas:
cestas_df_grouped = cestas_df_grouped.withColumn(
    "pct_entrega", F.col("qtde_entrega") / F.col("qtde_vendas")
)

# COMMAND ----------

#  display(cestas_df_grouped)

# COMMAND ----------

# metodo_pgto_preferido:
colunas_metodo_pagamento = [col for col in cestas_df_grouped.columns if 'pgto' in col]

cestas_df_grouped = calcula_coluna_maior_valor(
    cestas_df_grouped, colunas_metodo_pagamento
).withColumnRenamed('coluna_mais_frequente', 'metodo_pgto_preferido')

# COMMAND ----------

cestas_joined = cestas_df.join(
    cestas_df_grouped,
    on="concatenado",
    how="left",
)
# cestas_joined.display()

# COMMAND ----------

# Agora unir `vendas_df` e `cestas_joined` usando "pre_venda_id"
cestas_joined = cestas_joined.withColumnRenamed("basket_id", "pre_venda_id")

vendas_cestas_joined = vendas_df.join(
    cestas_joined.select(
        "pre_venda_id",
        "pct_pgto_pix",
        "pct_pgto_cartao",
        "pct_pgto_crediario",
        "pct_pgto_dinheiro",
        "pct_entrega",
        "pct_venda_agregada",
        "pct_venda_organica",
        "pct_venda_substituicao",
        "pct_itens_medicamentos_genericos",
        "pct_itens_medicamentos_referencia",
        "pct_itens_medicamentos_similar",
        "pct_itens_medicamentos_similar_intercambiavel",
        "metodo_pgto_preferido",
    ),
    on=["pre_venda_id"],
    how="left",
)

# agrupar por 'cpf_cliente' e conta_loja
vendas_cestas_joined = vendas_cestas_joined.groupBy("cpf_cliente", "conta_loja").agg(
    F.first("pct_pgto_pix").alias("pct_pgto_pix"),
    F.first("pct_pgto_cartao").alias("pct_pgto_cartao"),
    F.first("pct_pgto_crediario").alias("pct_pgto_crediario"),
    F.first("pct_pgto_dinheiro").alias("pct_pgto_dinheiro"),
    F.first("pct_entrega").alias("pct_canal_tele_entrega"),
    F.first("pct_venda_agregada").alias("pct_venda_agregada"),
    F.first("pct_venda_organica").alias("pct_venda_organica"),
    F.first("pct_venda_substituicao").alias("pct_venda_substituicao"),
    F.first("pct_itens_medicamentos_genericos").alias(
        "pct_itens_medicamentos_genericos"
    ),
    F.first("pct_itens_medicamentos_referencia").alias(
        "pct_itens_medicamentos_referencia"
    ),
    F.first("pct_itens_medicamentos_similar").alias("pct_itens_medicamentos_similar"),
    F.first("pct_itens_medicamentos_similar_intercambiavel").alias(
        "pct_itens_medicamentos_similar_intercambiavel"
    ),
    F.first("metodo_pgto_preferido").alias("metodo_pgto_preferido"),
)

# drop qualquer linha em que "cpf_cliente" ou "conta_loja" forem nulos
vendas_cestas_joined = vendas_cestas_joined.filter(F.col("cpf_cliente").isNotNull())
vendas_cestas_joined = vendas_cestas_joined.filter(F.col("conta_loja").isNotNull())

# Define "pct_canal_loja_fisica" como sendo (1 - pct_canal_tele_entrega)
vendas_cestas_joined = vendas_cestas_joined.withColumn(
    "pct_canal_loja_fisica", 1 - F.coalesce(F.col("pct_canal_tele_entrega"), F.lit(0))
)

# Adicionar colunas zeradas para pct_canal_e_commerce e pct_canal_outros_canais
# (não calculadas no momento, mas necessárias para o schema)
vendas_cestas_joined = vendas_cestas_joined.withColumn(
    "pct_canal_e_commerce", F.lit(0.0)
)
vendas_cestas_joined = vendas_cestas_joined.withColumn(
    "pct_canal_outros_canais", F.lit(0.0)
)

# Calcular canal_preferido usando as colunas de percentual de canal
# Com preferência para canais físicos (balcão, presencial) em caso de empate
colunas_canal = [
    "pct_canal_loja_fisica",
    "pct_canal_tele_entrega",
    "pct_canal_e_commerce",
    "pct_canal_outros_canais",
]

vendas_cestas_joined = calcula_coluna_maior_valor(vendas_cestas_joined, colunas_canal)

# Mapear os nomes das colunas para nomes mais amigáveis e aplicar regra de preferência
vendas_cestas_joined = vendas_cestas_joined.withColumn(
    "canal_preferido",
    F.when(F.col("coluna_mais_frequente") == "pct_canal_loja_fisica", "loja_fisica")
    .when(F.col("coluna_mais_frequente") == "pct_canal_tele_entrega", "tele_entrega")
    .when(F.col("coluna_mais_frequente") == "pct_canal_e_commerce", "e_commerce")
    .when(F.col("coluna_mais_frequente") == "pct_canal_outros_canais", "outros_canais")
    .otherwise(F.lit(None)),
).drop("coluna_mais_frequente")

# Tratar colunas CPF e conta
vendas_cestas_joined = vendas_cestas_joined.withColumn(
    "cpf_cliente", F.regexp_replace("cpf_cliente", "[^0-9]", "")
)
vendas_cestas_joined = vendas_cestas_joined.withColumnRenamed("cpf_cliente", "cpf_cnpj")
vendas_cestas_joined = vendas_cestas_joined.withColumnRenamed("conta_loja", "conta")


# COMMAND ----------

clientes_cestas_joined_df = clientes_rfm_joined_df.join(
    vendas_cestas_joined,
    on=["cpf_cnpj", "conta"],
    how='left',
)
# clientes_cestas_joined_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecimento tags
# MAGIC

# COMMAND ----------

tags_df = spark.read.parquet(s3_clientes_tags_folder).cache()

# TODO: precisa mesmo do drop_duplicates?
tags_df = tags_df.dropDuplicates(["id"])
tags_df = tags_df.drop("cpf_cnpj", "atualizado_em")
tags_df = tags_df.withColumnRenamed("id", "id_tags")

tags_joined_df = clientes_cestas_joined_df.join(
    tags_df, tags_df.id_tags == clientes_cestas_joined_df.id, "left"
)

tags_joined_df = tags_joined_df.drop("id_tags")

# tags_joined_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ajusta para o schema desejado

# COMMAND ----------

print(schema_clientes_conta_refined)

# COMMAND ----------

final_df = to_schema(tags_joined_df, schema_clientes_conta_refined)

# COMMAND ----------

# Teste
final_df.limit(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva no s3 e delta

# COMMAND ----------


if FULL_REFRESH:
    # NOTE: o `full_refresh` é útil quando alteramos o schema da tabela,
    #       mas lembre-se de usar o liquibase pra registrar a migração
    final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        DELTA_TABLE_REFINED
    )
else:
    final_df.write.mode("overwrite").saveAsTable(DELTA_TABLE_REFINED)

# COMMAND ----------

# TODO: Normalizar coluna "celular" para o formato '(99) 91111-1111'
