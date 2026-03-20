# Databricks notebook source
# MAGIC %md
# MAGIC # Clusterização de Lojas
# MAGIC - Tamanho de Loja: Fechar ranges para PP, P, M, G
# MAGIC - Geografia: Usar cadastro de lojas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import time
from datetime import date
from functools import reduce

import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
import requests
from pyspark.context import SparkConf
from pyspark.sql import SparkSession, Window

from maggulake.clientes.normaliza_endereco import normalize_address
from maggulake.io.postgres import PostgresAdapter
from maggulake.schemas import schema_clusterizacao_lojas
from maggulake.utils.pyspark import to_schema

# COMMAND ----------

print(schema_clusterizacao_lojas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Environment

# COMMAND ----------

config = SparkConf().setAll(pairs=[('spark.sql.caseSensitive', 'true')])

spark = (
    SparkSession.builder.appName('clusterizacao_lojas')
    .config(conf=config)
    .getOrCreate()
)

stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# delta tables
tabela_clientes_conta = f"{catalog}.standard.clientes_conta_standard"
tabela_clusterizacao_lojas = f'{catalog}.analytics.clusterizacao_lojas'
df_vendas_path = f'{catalog}.analytics.view_vendas'
view_cestas_de_compras_path = f'{catalog}.analytics.view_cestas_de_compras'
tabela_produtos_refined_path = f"{catalog}.refined.produtos_refined"

# postgres
USER = dbutils.secrets.get(scope='postgres', key='POSTGRES_USER')
PASSWORD = dbutils.secrets.get(scope='postgres', key='POSTGRES_PASSWORD')

# airtable
AIRTABLE_API_KEY_GUILHERME = dbutils.secrets.get(
    scope="databricks", key="AIRTABLE_API_KEY_GUILHERME"
)

# COMMAND ----------

postgres = PostgresAdapter(stage, USER, PASSWORD, utilizar_read_replica=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remover lojas dummy

# COMMAND ----------

lojas_ativas = (
    postgres.get_lojas(spark)
    .select(F.col('id').alias('loja_id'))
    .rdd.flatMap(lambda row: row)
    .collect()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de Tabelas necessárias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lojas

# COMMAND ----------

df_lojas = (
    postgres.read_table(spark, table="contas_loja")
    .select(
        'id',
        'cnpj',
        'ativo',
    )
    .withColumnRenamed('id', 'loja_id')
)


# COMMAND ----------

# df_lojas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extrato pontos

# COMMAND ----------

df_extrato_pontos = (
    postgres.read_table(spark, table='gameficacao_extratodepontos')
    .select(
        "item_da_venda_id",
        "produto_id",
        "jogador_id",
        "missao_id",
        "rodada_id",
        "missao_produto_id",
        "pontos_totais",
    )
    .filter(F.col('rodada_id').isNotNull())
    .filter(F.col('missao_produto_id').isNotNull())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saldo Pontos e Moedas

# COMMAND ----------

df_saldo_pontos = postgres.read_table(spark, table="gameficacao_saldodepontos").select(
    "missao_id",
    "rodada_id",
    "jogador_id",
    "nivel_atual",
    "pontos_totais",
)

# Cria um numero expressando a quantidade de niveis que o jogador tem
df_saldo_pontos = df_saldo_pontos.withColumn(
    "nivel", F.regexp_replace("nivel_atual", "nivel-", "").cast("integer")
).withColumn("nivel", F.coalesce(F.col("nivel"), F.lit(0)))

# COMMAND ----------

df_saldo_moedas = postgres.read_table(spark, table='gameficacao_saldodemoedas').select(
    'jogador_id',
    'moedas_totais',
)

# COMMAND ----------

# Seleciona somente a loja em que o atendente mais vendeu produtos.
# Nao podemos fazer o double count de pontos para os jogadores que tem pontos em mais de uma loja.
# TODO: da pra mover isso pro BigQueryAdapter
df_jogador_loja = postgres.execute_query("""
SELECT
  jogador_id,
  loja_id,
  qtd_vendida
FROM (
  SELECT
    ge.jogador_id,
    vv.loja_id,
    sum(vi.quantidade) as qtd_vendida,
    row_number() over (partition by ge.jogador_id order by sum(vi.quantidade) desc) as rn
  FROM
    gameficacao_extratodepontos ge
      INNER JOIN
    vendas_item vi on ge.item_da_venda_id = vi.id
      INNER JOIN
    vendas_venda vv on vi.venda_id = vv.id
  GROUP BY
    ge.jogador_id, vv.loja_id
)
where rn = 1
""")

print(f"Temos um total de {len(df_jogador_loja)} jogadores (i.e. atendentes)")

# Converter para pyspark
df_jogador_loja = spark.createDataFrame(df_jogador_loja)


# COMMAND ----------

df_lojas_niveis = (
    df_jogador_loja.join(
        df_saldo_pontos,
        on=["jogador_id"],
        how="inner",
    )
    .groupBy("loja_id")
    .agg(
        F.sum("nivel").alias("quantidade_lvlup"),
    )
)

# COMMAND ----------

df_lojas_moedas = (
    df_jogador_loja.join(
        df_saldo_moedas,
        on=["jogador_id"],
        how="inner",
    )
    .groupBy("loja_id")
    .agg(
        F.sum("moedas_totais").alias("moedas_totais"),
    )
)

# df_lojas_moedas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vendas

# COMMAND ----------

df_vendas = (
    spark.read.table(df_vendas_path)
    .withColumnRenamed('venda_item_id', 'item_da_venda_id')
    .withColumn('realizada_em', F.expr("realizada_em - interval 3 hours"))
    .select(
        'venda_id',
        'item_da_venda_id',
        'produto_id',
        'loja_id',
        'realizada_em',
        'quantidade',
        'preco_venda_desconto',
        'nome_loja',
        'conta_loja',
        'codigo_loja',
        'vendedor_id',
        'username_vendedor',
    )
    .filter(F.col('loja_id').isin(lojas_ativas))
).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tempo uso

# COMMAND ----------


df_tempo_uso = (
    spark.read.table(view_cestas_de_compras_path)
    .filter(F.col('store_id').isin(lojas_ativas))
    .withColumn('closed_at', F.expr("closed_at - interval 3 hours"))
    .groupBy('store_id')
    .agg(
        F.min('closed_at').alias('data_cesta_fechada'),
        F.min('opened_at').alias('opened_at'),
    )
    .withColumn(
        'primeira_cesta_fechada_em',
        F.to_date(F.coalesce('data_cesta_fechada', 'opened_at')),
    )
    .withColumn('today', F.lit(date.today()))
    .withColumn("tempo_de_uso", F.date_diff('today', 'data_cesta_fechada'))
    .select(
        F.col('store_id').alias('loja_id'),
        'primeira_cesta_fechada_em',
        'tempo_de_uso',
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo das métricas:
# MAGIC
# MAGIC - Número de níveis atingidos por antendente
# MAGIC - Calcular tamanho (faturamento)
# MAGIC - Número de atendentes,
# MAGIC - Horário de pico,
# MAGIC - Quantidade de atendimentos,
# MAGIC - Ticket médio,
# MAGIC - Proporções RX/OTC
# MAGIC - Faturamento médio mensal

# COMMAND ----------

# MAGIC %md
# MAGIC ### Maturidade

# COMMAND ----------

# Calcula pontos roxos totais
df_maturidade = (
    df_vendas.join(df_extrato_pontos, how="left", on=["item_da_venda_id"])
    .groupBy("loja_id", "nome_loja", "conta_loja", "codigo_loja")
    .agg(
        F.sum(df_extrato_pontos.pontos_totais).alias("pontos_roxos_totais"),
    )
)

# Agrega total de level ups
df_maturidade = df_maturidade.join(
    df_lojas_niveis,
    on=["loja_id"],
    how="inner",
)

# Agrega total de moedas (i.e. magguletes)
df_maturidade = df_maturidade.join(
    df_lojas_moedas,
    on=["loja_id"],
    how="inner",
)

# Agrega infos cadastrais
df_maturidade = df_maturidade.join(
    df_lojas,
    on=["loja_id"],
    how="inner",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Faturamento mensal

# COMMAND ----------

# tamanho faturamento quantidade atendimentos e atendentes mensal
df_intermediario_mensal = (
    df_vendas.withColumn('ano', F.year('realizada_em'))
    .withColumn('mes', F.month('realizada_em'))
    .withColumn('concatenado', F.concat_ws('-', F.col('ano'), F.col('mes')))
    .withColumn(
        'dias_por_mes',
        F.date_diff(F.last_day('realizada_em'), F.date_trunc('month', 'realizada_em')),
    )
    .withColumn('faturamento', F.col('quantidade') * F.col('preco_venda_desconto'))
    .groupBy('loja_id', 'concatenado')
    .agg(
        F.round(F.sum('faturamento'), 2).alias('tamanho_faturamento_mensal'),
        F.count_distinct('venda_id').alias('quantidade_atendimentos_mensal'),
        F.first('dias_por_mes').alias('dias_por_mes'),
        F.count_distinct('vendedor_id').alias('quantidade_atendentes_mensal'),
    )
)

df_metricas_gerais = df_vendas.groupBy('loja_id').agg(
    F.count_distinct(
        F.when(
            F.datediff(F.current_date(), F.col('realizada_em')) <= 35,
            F.col('vendedor_id'),
        )
    ).alias('quantidade_atendentes'),
    F.count_distinct('venda_id').alias('quantidade_atendimentos_total'),
)

df_medio_mensal = (
    df_intermediario_mensal.withColumn(
        "ticket_medio",
        F.round(
            (
                F.col('tamanho_faturamento_mensal')
                / F.col("quantidade_atendimentos_mensal")
            ),
            2,
        ),
    )
    .withColumn(
        "tamanho_faturamento",
        F.round((F.col('tamanho_faturamento_mensal') / F.col("dias_por_mes")), 2),
    )
    .withColumn(
        "quantidade_atendimentos",
        F.round((F.col('quantidade_atendimentos_mensal') / F.col("dias_por_mes")), 2),
    )
    .groupBy('loja_id')
    .agg(
        F.round(F.avg('tamanho_faturamento'), 2).alias(
            'tamanho_faturamento_medio_mensal'
        ),
        F.round(F.avg('ticket_medio'), 2).alias('ticket_medio_mensal'),
        F.round(F.avg('quantidade_atendimentos'), 2).alias(
            'quantidade_atendimentos_medio_mensal'
        ),
    )
    .join(df_metricas_gerais, on='loja_id', how='left')
)

# COMMAND ----------

# faturamento mensal
df_fat_mensal = (
    df_vendas.withColumn(
        'faturamento', F.col('quantidade') * F.col('preco_venda_desconto')
    )
    .withColumn('ano_mes', F.date_format('realizada_em', 'yyyy-MM'))
    .groupBy('loja_id', 'ano_mes')
    .agg(F.sum('faturamento').alias('faturamento_mensal'))
    .groupBy('loja_id')
    .agg(F.round(F.avg('faturamento_mensal'), 2).alias('faturamento_medio_mensal'))
    .select('loja_id', 'faturamento_medio_mensal')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Horario de pico

# COMMAND ----------

# horario de pico
window_spec = Window.partitionBy("loja_id").orderBy(F.desc("quantidade"))

df_horario_pico = (
    df_vendas.withColumn('horario_de_pico', F.hour('realizada_em'))
    .groupBy('loja_id', 'horario_de_pico')
    .agg(F.sum('quantidade').alias('quantidade'))
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") == 1)
    .drop('quantidade', 'rank')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Proporcao RX/OTC

# COMMAND ----------

# informacoes adicionais tabela produto
df_produtos_refined = (
    spark.read.table(tabela_produtos_refined_path)
    .withColumnRenamed('id', 'produto_id')
    .select(
        'produto_id',
        'marca',
        'fabricante',
        'eh_otc',
        'eh_medicamento',
        'tipo_de_receita_completo',
    )
    .filter(F.col("eh_medicamento").isNotNull())
    .filter(F.col("eh_otc").isNotNull())
)

# COMMAND ----------

# proporcao RX/OTC
window_spec_produto = Window.partitionBy('loja_id')
df_rx_otc = (
    df_vendas.join(df_produtos_refined, how='inner', on='produto_id')
    .select('loja_id', 'produto_id', 'eh_otc', 'eh_medicamento')
    .distinct()
    .withColumn("qtd_produtos", F.count('produto_id').over(window_spec_produto))
    .withColumn(
        "qtd_produtos_com_receita",
        F.when(
            ((F.col('eh_otc') == False) & (F.col('eh_medicamento') == True)), F.lit(1)
        ),
    )
    .withColumn(
        "qtd_produtos_sem_receita",
        F.when(
            ((F.col('eh_otc') == True) & (F.col('eh_medicamento') == True))
            | (F.col('eh_medicamento') == False),
            F.lit(1),
        ),
    )
    .groupBy('loja_id')
    .agg(
        F.first('qtd_produtos').alias('quantidade_total'),
        F.sum("qtd_produtos_com_receita").alias('qtd_produtos_com_receita'),
        F.sum("qtd_produtos_sem_receita").alias('qtd_produtos_sem_receita'),
    )
    .withColumn(
        'percentual_rx',
        F.round(F.col('qtd_produtos_com_receita') / F.col('quantidade_total'), 2),
    )
    .withColumn(
        'percentual_otc',
        F.round(F.col('qtd_produtos_sem_receita') / F.col('quantidade_total'), 2),
    )
    .select('loja_id', 'percentual_rx', 'percentual_otc')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agrega todos os dataframes

# COMMAND ----------

df_intermediario = (
    df_maturidade.join(df_medio_mensal, how='left', on=['loja_id'])
    .join(df_fat_mensal, how='left', on=['loja_id'])
    .join(df_horario_pico, how='left', on=['loja_id'])
    .join(df_rx_otc, how='left', on=['loja_id'])
    .join(df_tempo_uso, how='left', on=['loja_id'])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enriquece com informacoes de Latitude e Longitude

# COMMAND ----------

# parquet contendo as lat e long das lojas
df_lat_long = spark.read.parquet("/tmp/maggu/lojas/coordenadas")

# TODO: print quantidade


# COMMAND ----------

df_final = df_intermediario.join(df_lat_long, how='left', on='loja_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição Regra de tamanho
# MAGIC - G se ao menos 2 métricas ≥ Q3
# MAGIC - M se ao menos 2 métricas ≥ median
# MAGIC - P se ao menos 1 métrica ≥ Q1
# MAGIC - PP caso contrário

# COMMAND ----------

# Métricas para classificação de tamanho de loja:
# - quantidade_atendentes: total de vendedores distintos por loja (em todo o período)
# - quantidade_atendimentos_medio_mensal: média diária de vendas por mês
# - quantidade_atendimentos_total: número total de vendas (em todo o período)
metrics = [
    'faturamento_medio_mensal',
    'quantidade_atendentes',
    'quantidade_atendimentos_total',
]

# calcula percentil para cada metrica
thresholds = {}
for m in metrics:
    quantiles = (
        df_final.approxQuantile(m, [0.25, 0.50, 0.75], 0.0)
        if m in df_final.columns
        else (None, None, None)
    )
    q1, q2, q3 = quantiles if len(quantiles) == 3 else (None, None, None)
    thresholds[m] = (q1, q2, q3)

# contagem de métricas por cada percentil atingido
cnt_q3 = reduce(
    lambda acc, m: acc + (F.col(m) >= F.lit(thresholds[m][2])).cast("int"),
    metrics,
    F.lit(0),
)
cnt_med = reduce(
    lambda acc, m: acc + (F.col(m) >= F.lit(thresholds[m][1])).cast("int"),
    metrics,
    F.lit(0),
)
cnt_q1 = reduce(
    lambda acc, m: acc + (F.col(m) >= F.lit(thresholds[m][0])).cast("int"),
    metrics,
    F.lit(0),
)

# COMMAND ----------

df_final = (
    df_final.withColumn('cnt_q3', cnt_q3)
    .withColumn('cnt_med', cnt_med)
    .withColumn('cnt_q1', cnt_q1)
    .withColumn(
        'tamanho_loja',
        F.when(F.col('cnt_q3') >= 2, F.lit('G'))
        .when(F.col('cnt_med') >= 2, F.lit('M'))
        .when(F.col('cnt_q1') >= 1, F.lit('P'))
        .otherwise(F.lit('PP')),
    )
    .drop('cnt_q3', 'cnt_med', 'cnt_q1')
)

# COMMAND ----------

# TODO: Com o desligamento do airtable, precisamos atualizar essa parte para ler direto do banco.

# df_lojas_airtable_pandas = get_lojas_df(AIRTABLE_API_KEY_GUILHERME)
# TODO: não vai funcionar, mas deixei aqui pra quem conseguir refatorar esse código depois...
df_lojas_airtable_pandas = pd.DataFrame()
df_lojas_airtable_spark = (
    spark.createDataFrame(df_lojas_airtable_pandas)
    .withColumnRenamed('loja_id_copilot', 'loja_id')
    .filter(F.col('loja_id').isNotNull())
    .fillna('Brasil', subset='País')
    .select(
        'loja_id',
        'Endereço Completo',
        'CEP',
        F.col('Cidade').alias('cidade'),
        F.col('Estado').alias('estado'),
        'País',
    )
)

# COMMAND ----------

# adiciona info de cidade e estado
df_final = df_final.join(
    df_lojas_airtable_spark.select('loja_id', 'cidade', 'estado'),
    how='left',
    on=['loja_id'],
).withColumn('created_at', F.current_timestamp())

# COMMAND ----------

df_novas_lojas = df_lojas_airtable_spark.join(df_lat_long, how='leftanti', on='loja_id')
lojas_pendentes: list = df_novas_lojas.collect()

# COMMAND ----------

if not len(lojas_pendentes) > 0:
    print("Salvando lojas atualizadas na tabela com o seguinte schema:")
    df_final.printSchema()
    df_final = to_schema(
        df_final,
        schema=schema_clusterizacao_lojas,
    )
    df_final.write.mode('overwrite').saveAsTable(tabela_clusterizacao_lojas)
    dbutils.notebook.exit("Todas as lojas estao contempladas na análise")
else:
    print(
        "Algumas lojas ainda precisam ser refinadas, vamos continuar com o notebook..."
    )


# COMMAND ----------

normalize_address_udf = F.udf(normalize_address, T.StringType())

# COMMAND ----------

df_novas_lojas = df_novas_lojas.withColumn(
    'Endereço com Número', normalize_address_udf('Endereço com Número')
).withColumn(
    'endereco_normalizado',
    F.when(
        F.col("Endereço com Número").isNotNull(),
        F.concat_ws(
            ",",
            F.col("Endereço com Número"),
            F.col("Cidade"),
            F.col("Estado"),
            F.col("País"),
        ),
    ).otherwise(
        F.concat_ws(",", F.col("CEP"), F.col("Cidade"), F.col("Estado"), F.col("País"))
    ),
)
novas_lojas = df_novas_lojas.select('loja_id', 'endereco_normalizado').collect()

# COMMAND ----------

# Consulta latitude e longitude de novas lojas


def popula_lat_long(records):
    url = 'https://nominatim.openstreetmap.org/search'
    headers = {'User-Agent': 'geolocalizacao'}
    session = requests.Session()

    results = []
    for rec in records:
        query = rec.endereco_normalizado
        print(f"Geocoding loja_id={rec.loja_id}: {query!r}")

        params = {'format': 'json', 'limit': 1, 'q': query}
        try:
            resp = session.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data:
                lat = float(data[0]['lat'])
                lon = float(data[0]['lon'])
            else:
                print(f"No result for loja_id={rec.loja_id}")
                lat = lon = None

        except requests.exceptions.RequestException as e:
            print(f"Request failed for loja_id={rec.loja_id}: {e}")
            lat = lon = None

        results.append({'loja_id': rec.loja_id, 'latitude': lat, 'longitude': lon})
        time.sleep(1)

    return results


# COMMAND ----------

results = popula_lat_long(novas_lojas)

# COMMAND ----------

localizacao_schema = T.StructType(
    [
        T.StructField('latitude', T.DoubleType()),
        T.StructField('longitude', T.DoubleType()),
        T.StructField('loja_id', T.StringType()),
    ]
)
df_localizacao = spark.createDataFrame(results, localizacao_schema)

# COMMAND ----------

# Salva novas lojas cadastradas
df_localizacao.write.mode("append").parquet("/tmp/maggu/lojas/coordenadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salva  informações

# COMMAND ----------

df_updated_lat_long = spark.read.parquet("/tmp/maggu/lojas/coordenadas")

# COMMAND ----------

df_final = (
    df_intermediario.join(df_updated_lat_long, how='left', on=['loja_id'])
    .join(
        df_lojas_airtable_spark.select('loja_id', 'cidade', 'estado'),
        how='left',
        on=['loja_id'],
    )
    .withColumn('created_at', F.current_timestamp())
)

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_final = to_schema(
    df_final,
    schema=schema_clusterizacao_lojas,
)

# COMMAND ----------

df_final.write.mode('overwrite').saveAsTable(tabela_clusterizacao_lojas)
