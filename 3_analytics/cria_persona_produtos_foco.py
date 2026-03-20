# Databricks notebook source
# MAGIC %md
# MAGIC ## Configurações

# COMMAND ----------

dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.context import SparkConf
from pyspark.sql import SparkSession

from maggulake.environment.tables import Table
from maggulake.io.postgres import PostgresAdapter
from maggulake.utils.elemento_frequente import most_frequent

# COMMAND ----------

config = SparkConf().setAll(pairs=[('spark.sql.caseSensitive', 'true')])

spark = SparkSession.builder.appName('Persona ads').config(conf=config).getOrCreate()

# por enquanto só refinamos em prod, pois não tem vendas em staging
# STAGE = dbutils.widgets.get("stage")
stage = "prod"
CATALOG = "production" if stage == "prod" else "staging"
FULL_REFRESH = dbutils.widgets.get("full_refresh") == "true"

# leitura de tabelas usadas no script
tabela_vendas = Table.view_vendas.value
tabela_clientes_refined = Table.clientes_conta_refined.value
tabela_cestas = Table.view_cestas_de_compras.value
tabela_produtos = Table.produtos_refined.value

# Conexao com o postgres para leitura dos produtos em campanha
USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')
postgres = PostgresAdapter(stage, USER, PASSWORD)


# Variavel para salvamento no catalogo delta
delta_refined_path = f"{CATALOG}.refined.produtos_ads_persona"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura produtos ads

# COMMAND ----------

produtos_em_campanha_list = postgres.get_eans_em_campanha(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura cadastro refined produtos

# COMMAND ----------

refined_produtos_df = spark.read.table(tabela_produtos)
refined_produtos_ids_df = refined_produtos_df.select('ean', 'id')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lista de eans alternativos para o mesmo ean principal cadastrado

# COMMAND ----------

# Leitura de eans alternativos para eans que possuem eans alternativos
eans_alternativos = (
    refined_produtos_df.filter(F.col('ean').isin(produtos_em_campanha_list))
    .select('eans_alternativos')
    .filter(F.size(F.col('eans_alternativos')) > 0)
    .withColumn('eans_alternativos', F.explode(F.col('eans_alternativos')))
    .select('eans_alternativos')
    .rdd.flatMap(lambda row: row)
    .collect()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das informações de Vendas

# COMMAND ----------

# Leitura da tabela de vendas
vendas_df = spark.read.table(tabela_vendas)

# Remover linhas em que a coluna `cpf_cliente` está vazia
vendas_df = vendas_df.filter(vendas_df.cpf_cliente.isNotNull())

# Remover pontos e traços da coluna cpf_cnpj_cliente
vendas_df = vendas_df.withColumn(
    "cpf_cnpj_cliente", F.regexp_replace("cpf_cnpj_cliente", "[^0-9]", "")
)
# Padroniza a coluna cpf_cnpj
vendas_df = vendas_df.withColumnRenamed('cpf_cnpj_cliente', 'cpf_cnpj')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informação de Vendas de eans alternativos
# MAGIC - Agregar info do ean alternativo no ean principal cadastrado

# COMMAND ----------

# Leitura
vendas_alternativos_df = vendas_df.filter(F.col('produto_ean').isin(eans_alternativos))

# COMMAND ----------

# df contendo ean principal e eans alternativos
de_para_produtos_df = (
    spark.read.table(tabela_produtos)
    .filter(F.col('ean').isin(eans_alternativos))
    .filter(F.size(F.col('eans_alternativos')) > 0)
    .select('ean', F.explode('eans_alternativos').alias('ean_alternativo'))
)
# Consolidando a informação do ean alternativo no ean principal
vendas_alternativos_df = vendas_alternativos_df.join(
    de_para_produtos_df,
    vendas_alternativos_df.produto_ean == de_para_produtos_df.ean_alternativo,
    'left',
)
# Selecionando apenas colunas necessárias
vendas_alternativos_df = vendas_alternativos_df.select(
    'ean',
    'realizada_em',
    'venda_id',
    'venda_item_id',
    'preco_venda_desconto',
    'cpf_cnpj',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informação de Vendas do ean principal

# COMMAND ----------

vendas_ean_foco_df = vendas_df.filter(
    F.col('produto_ean').isin(produtos_em_campanha_list)
).select(
    F.col('produto_ean').alias('ean'),
    'realizada_em',
    'venda_id',
    'venda_item_id',
    'preco_venda_desconto',
    'cpf_cnpj',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unificando as informações de eans alternativos no ean principal

# COMMAND ----------

vendas_df = vendas_ean_foco_df.unionByName(vendas_alternativos_df)

# COMMAND ----------

if vendas_df.count() != vendas_ean_foco_df.count() + vendas_alternativos_df.count():
    raise ValueError(
        f"Contagem esperada : vendas_df.count() = {vendas_df.count()}, "
        f"Diff na contagem = {vendas_ean_foco_df.count()} + {vendas_alternativos_df.count()}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das informações da tabela cria_clientes_refined

# COMMAND ----------

# leitura das informações enriquecidas de cliente
clientes_refined_df = spark.read.table(tabela_clientes_refined).select(
    'cpf_cnpj', 'faixa_etaria', 'sexo', 'idade'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de informações para a criação da persona
# MAGIC - Informações semelhantes utilizadas no cálculo de RFM
# MAGIC - Informações adicionais da cesta

# COMMAND ----------

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

# COMMAND ----------

produtos_info_metricas_df = vendas_df.groupBy('ean', 'cpf_cnpj').agg(
    F.min("realizada_em").alias("data_primeira_compra"),
    F.max("realizada_em").alias("data_ultima_compra"),  # Recência
    F.sum("preco_venda_desconto").alias("valor_total"),  # Valor Monetário
    F.countDistinct("venda_id").alias("total_de_compras"),  # Frequência
    F.countDistinct("venda_item_id").alias("total_de_itens"),
    F.collect_list("venda_id").alias("compras"),
    F.mode("dia_do_mes").alias("pref_dia_mes"),
    F.mode("hora_do_dia").alias("pref_hora"),
    F.mode("dia_da_semana").alias("pref_dia_semana"),
)

# COMMAND ----------

# ticket_medio
produtos_info_metricas_df = produtos_info_metricas_df.withColumn(
    "ticket_medio",
    F.round(
        (
            produtos_info_metricas_df.valor_total
            / produtos_info_metricas_df.total_de_compras
        ),
        2,
    ),
)

# COMMAND ----------

# converter timestamp para date
produtos_info_metricas_df = produtos_info_metricas_df.withColumn(
    "data_ultima_compra",
    F.to_date(produtos_info_metricas_df.data_ultima_compra),
)
produtos_info_metricas_df = produtos_info_metricas_df.withColumn(
    "data_primeira_compra",
    F.to_date(produtos_info_metricas_df.data_primeira_compra),
)


# 'intervalo_medio_entre_compras'
produtos_info_metricas_df = produtos_info_metricas_df.withColumn(
    "intervalo_medio_entre_compras",
    F.datediff(
        produtos_info_metricas_df.data_ultima_compra,
        produtos_info_metricas_df.data_primeira_compra,
    )
    / produtos_info_metricas_df.total_de_compras,
).withColumn(
    "intervalo_medio_entre_compras", F.col("intervalo_medio_entre_compras").cast("int")
)


# 'media_itens_por_cesta'
produtos_info_metricas_df = produtos_info_metricas_df.withColumn(
    "media_itens_por_cesta",
    F.round(
        (
            produtos_info_metricas_df.total_de_itens
            / produtos_info_metricas_df.total_de_compras
        ).cast("float"),
        2,
    ),
)

# COMMAND ----------

grouped_produtos_info_metricas_df = (
    produtos_info_metricas_df.join(clientes_refined_df, how='left', on=['cpf_cnpj'])
    # Calculando quantas vendas por cpf foram únicas
    .withColumn(
        'eh_compra_unica', F.when(F.size(F.col('compras')) == 1, 1).otherwise(0)
    )
    .groupBy('ean')
    .agg(
        F.min("data_primeira_compra").alias("data_primeira_compra"),
        F.max("data_ultima_compra").alias("data_ultima_compra"),  # Recência
        F.sum("valor_total").alias("valor_total"),  # Valor Monetário
        F.sum("total_de_compras").alias("total_de_compras"),  # Frequência
        F.sum("total_de_itens").alias("total_de_itens"),
        F.flatten(F.collect_list("compras")).alias("compras"),
        F.mode("pref_dia_mes").alias("pref_dia_mes"),
        F.mode("pref_hora").alias("pref_hora"),
        F.mode("pref_dia_semana").alias("pref_dia_semana"),
        F.avg('ticket_medio').alias('ticket_medio'),
        F.avg('media_itens_por_cesta').alias('media_itens_por_cesta'),
        F.avg('intervalo_medio_entre_compras').alias('intervalo_medio_entre_compras'),
        F.mode('faixa_etaria').alias('faixa_etaria'),
        F.mode('sexo').alias('sexo'),
        F.mode('idade').alias('idade'),
        F.sum('eh_compra_unica').alias('eh_compra_unica'),
    )
    # percentual de compras únicas
    .withColumn(
        'pct_compra_unica',
        ((F.col('eh_compra_unica') / F.col('total_de_compras')) * 100).cast("float"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecendo com informações da Cesta

# COMMAND ----------

# Função para cálculo de valor mais frequente em uma lista
most_frequent_udf = F.udf(most_frequent, T.StringType())

# COMMAND ----------

# Leitura das informações de cesta
cesta_df = spark.read.table(tabela_cestas)

# Filtrando somente produtos ads
cesta_df = cesta_df.filter(F.col('product_ean').isin(produtos_em_campanha_list))

# Noramlizando nomenclatura para join
cesta_df = cesta_df.withColumnRenamed("product_ean", "ean").withColumnRenamed(
    'product_name', 'nome_produto'
)

# Normalizando coluna sintomas
cesta_df = cesta_df.withColumn(
    'symptoms', F.coalesce(F.col('symptoms'), F.array(F.lit('nao_informado')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Colunas auxiliares para cálculo de percentuais

# COMMAND ----------

# Colunas auxiliares para cálculo de percentual dos valores da coluna eh_venda
cesta_df = cesta_df.withColumn(
    'eh_venda_agregada', F.when(F.col('tipo_venda') == 'agregada', 1).otherwise(0)
)
cesta_df = cesta_df.withColumn(
    'eh_venda_substituicao',
    F.when(F.col('tipo_venda') == 'substituicao', 1).otherwise(0),
)
cesta_df = cesta_df.withColumn(
    'eh_venda_organica', F.when(F.col('tipo_venda') == 'organica', 1).otherwise(0)
)

# Colunas auxiliares para cálculo de percentual dos valores da coluna tipo_recomendação
cesta_df = cesta_df.withColumn(
    'eh_recomendacao_ia_padrao',
    F.when(F.col('tipo_recomendacao') == 'ia_padrao', 1).otherwise(0),
)
cesta_df = cesta_df.withColumn(
    'eh_recomendacao_atendente',
    F.when(F.col('tipo_recomendacao') == 'atendente', 1).otherwise(0),
)
cesta_df = cesta_df.withColumn(
    'eh_recomendacao_ia_ads',
    F.when(F.col('tipo_recomendacao') == 'ia_ads', 1).otherwise(0),
)
cesta_df = cesta_df.withColumn(
    'eh_recomendacao_ia_foco',
    F.when(F.col('tipo_recomendacao') == 'ia_foco', 1).otherwise(0),
)

# COMMAND ----------

# Selecionando as informações necessárias para o enriquecimento
cesta_df_grouped = (
    cesta_df.groupBy("ean", "nome_produto")
    .agg(
        most_frequent_udf(F.flatten(F.collect_list(F.col("symptoms")))).alias(
            "sintoma"
        ),
        F.mode("tipo_venda").alias("tipo_venda"),
        F.mode("tipo_recomendacao").alias("tipo_recomendacao"),
        F.sum("eh_venda_agregada").alias("eh_venda_agregada"),
        F.sum("eh_venda_substituicao").alias("eh_venda_substituicao"),
        F.sum("eh_venda_organica").alias("eh_venda_organica"),
        F.sum("eh_recomendacao_ia_padrao").alias("eh_recomendacao_ia_padrao"),
        F.sum("eh_recomendacao_atendente").alias("eh_recomendacao_atendente"),
        F.sum("eh_recomendacao_ia_ads").alias("eh_recomendacao_ia_ads"),
        F.sum("eh_recomendacao_ia_foco").alias("eh_recomendacao_ia_foco"),
        F.count(F.col('ean')).alias('total_compras'),
        F.mode('sale_channel').alias('canal_venda'),
        F.mode('payment_method').alias('metodo_pagamento'),
    )
    # Calculando percentuais das colunas eh_venda e eh_recomendacao
    .withColumn(
        'pct_venda_agregada',
        (F.col('eh_venda_agregada') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_venda_substituicao',
        (F.col('eh_venda_substituicao') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_venda_organica',
        (F.col('eh_venda_organica') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_venda_outros',
        F.abs(
            100
            - (
                F.col('pct_venda_agregada')
                + F.col('pct_venda_substituicao')
                + F.col('pct_venda_organica')
            )
        ),
    )
    .withColumn(
        'pct_eh_recomendacao_ia_padrao',
        (F.col('eh_recomendacao_ia_padrao') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_eh_recomendacao_atendente',
        (F.col('eh_recomendacao_atendente') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_eh_recomendacao_ia_ads',
        (F.col('eh_recomendacao_ia_ads') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_eh_recomendacao_ia_foco',
        (F.col('eh_recomendacao_ia_foco') / F.col('total_compras')) * 100,
    )
    .withColumn(
        'pct_eh_recomendacao_outros',
        F.abs(
            100
            - (
                F.col('pct_eh_recomendacao_ia_padrao')
                + F.col('pct_eh_recomendacao_atendente')
                + F.col('pct_eh_recomendacao_ia_ads')
                + F.col('pct_eh_recomendacao_ia_foco')
            )
        ),
    )
    .drop(
        'eh_venda_agregada',
        'eh_venda_substituicao',
        'eh_venda_organica',
        'eh_recomendacao_ia_padrao',
        'eh_recomendacao_atendente',
        'eh_recomendacao_ia_ads',
        'eh_recomendacao_ia_foco',
        'total_compras',
    )
)

# COMMAND ----------

persona_produtos_ads_df = (
    grouped_produtos_info_metricas_df.join(cesta_df_grouped, on="ean", how="left")
    .join(refined_produtos_ids_df, on="ean", how="left")
    # Data de atualização das informações
    .withColumn('data_atualização', F.current_timestamp())
)

# COMMAND ----------

if FULL_REFRESH:
    persona_produtos_ads_df.write.mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(delta_refined_path)
else:
    persona_produtos_ads_df.write.mode("overwrite").saveAsTable(delta_refined_path)
