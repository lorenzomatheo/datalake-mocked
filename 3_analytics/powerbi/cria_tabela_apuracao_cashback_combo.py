# Databricks notebook source
# MAGIC %pip install gspread google-auth

# COMMAND ----------

dbutils.widgets.dropdown("stage", "dev", ["dev", "prod"])

# COMMAND ----------

import datetime
import json

import gspread
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from google.oauth2.service_account import Credentials
from pyspark.context import SparkConf
from pyspark.sql import SparkSession, Window

from maggulake.io.postgres import PostgresAdapter
from maggulake.schemas import schema_sftp_tradefy

# COMMAND ----------

# spark
config = SparkConf().setAll([("spark.sql.caseSensitive", "true")])

spark = (
    SparkSession.builder.appName("adiciona_correcoes_manuais")
    .config(conf=config)
    .getOrCreate()
)

# ambiente
stage = dbutils.widgets.get("stage")
catalog = "production" if stage == "prod" else "staging"

# postgres
POSTGRES_USER = dbutils.secrets.get(scope="postgres", key="POSTGRES_USER")
POSTGRES_PASSWORD = dbutils.secrets.get(scope="postgres", key="POSTGRES_PASSWORD")
postgres = PostgresAdapter(
    stage, POSTGRES_USER, POSTGRES_PASSWORD, utilizar_read_replica=True
)

# google sheets credentials
json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
SHEET_ID = "1pllSEsUe-74UULG94avG5nSDQSwl2KPKNHGnkXx9w7s"
aba_opt_in_combo = "opt_in_combo"
info_combo = "info_combo"
aba_eans = 'lista_produtos'

# aws
aws_tradefy_path = 's3://maggu-datalake-prod/1-raw-layer/tradefy/'

# delta
apuracao_combo_table = 'hive_metastore.pbi.apuracao_cashback_combo'
produtos_refined_table = "production.refined.produtos_refined"

# COMMAND ----------

json_txt = dbutils.secrets.get("databricks", "GOOGLE_SHEETS_PBI")
creds = Credentials.from_service_account_info(
    json.loads(json_txt),
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

gc = gspread.authorize(creds)

# cashback combo

rows_aba_combo = gc.open_by_key(SHEET_ID).worksheet(aba_opt_in_combo).get_all_records()
rows_info_combo = gc.open_by_key(SHEET_ID).worksheet(info_combo).get_all_records()
df_opt_in_combo = spark.createDataFrame(rows_aba_combo)
df_info_combo = spark.createDataFrame(rows_info_combo)

# eans disponiveis para cashback
rows_eans = gc.open_by_key(SHEET_ID).worksheet(aba_eans).get_all_records()

# COMMAND ----------

# Define o escopo de eans disponiveis e redes que deram opt_in

redes_para_desconsiderar = [
    'farmacias_exclusivas'
]  # por enquanto somente essa por ser uma distribuidora
lista_eans = list(set(str(row['ean']) for row in rows_eans))
lista_rede_combo = list(
    str(row['rede'])
    for row in rows_aba_combo
    if row['rede'] not in redes_para_desconsiderar
)

# COMMAND ----------

lista_rede_combo

# COMMAND ----------

# Informações adicionais
df_info_conta = (
    postgres.read_table(spark, 'contas_loja')
    .filter(F.col("databricks_tenant_name").isin(lista_rede_combo))
    .select(
        F.col('id').alias('loja_id'),
        F.col('cnpj').alias("cnpj_loja"),
        F.col('databricks_tenant_name').alias('conta_loja'),
    )
)

df_info_produto = (
    spark.read.table(produtos_refined_table)
    .filter(F.col('ean').isin(lista_eans))
    .select('ean', 'nome', 'marca', 'fabricante')
)

# COMMAND ----------

df_periodo_apuracao = (
    df_opt_in_combo.withColumn('today', F.current_date())
    .withColumn('diff', F.date_diff(F.col("data_fim"), F.col('today')))
    .groupBy('data_inicio', 'data_fim')
    .agg(F.min("diff").alias("diff"))
    .drop('diff')
)

# COMMAND ----------

inicio_apuracao = (
    df_periodo_apuracao.select('data_inicio').first().asDict()['data_inicio']
)
fim_apuracao = df_periodo_apuracao.select('data_fim').first().asDict()['data_fim']

print(f'Periodo de apuração: {inicio_apuracao} a {fim_apuracao}')

# COMMAND ----------

df_notas_tradefy = (
    spark.read.csv(aws_tradefy_path, sep="|", header=True, schema=schema_sftp_tradefy)
    .dropDuplicates()  # importante remover duplicatas pq os arquivos são incrementais em uma janela rolante
    .select(
        F.col('CNPJ_Filial_Distribuidor').alias('cnpj_distribuidor'),
        F.col('CNPJ_PDV').alias('cnpj_loja'),
        F.col('Cod_Prod').alias('ean'),
        F.col('Data_Nota_Fiscal').alias('data_nota_fiscal'),
        F.col('Numero_Nota_Fiscal').alias('numero_nota_fiscal'),
        F.col('Tipo_Documento').alias('tipo_documento'),
        F.col('Venda_Unidades').alias('venda_unidades'),
        F.col('Venda_Liquida').alias('venda_liquida'),
    )
    .join(
        df_info_conta, how='inner', on='cnpj_loja'
    )  # somente redes com opt in na planilha
    .filter(F.col('tipo_documento') == 'F')  # somente notas faturadas
    .withColumn(
        "data_nota_fiscal",
        F.date_format(F.to_date(F.col('data_nota_fiscal'), "yyyyMMdd"), "yyyy-MM-dd"),
    )
)

# COMMAND ----------

# # Leitura de notas fiscais já apuradas da campanha combo
df_notas_fiscais_apuradas = (
    spark.read.table(apuracao_combo_table)
    .filter(
        F.col('tipo_campanha') == F.lit(f'combo - {inicio_apuracao}/{fim_apuracao}')
    )  # verificar somente notas da campanha vigente
    .withColumn('notas_fiscais', F.explode('notas_fiscais'))
    .groupBy(
        'conta_loja',
    )
    .agg(F.collect_set('notas_fiscais').alias('notas_fiscais'))
)

df_notas_nao_apuradas = (
    df_notas_tradefy.join(
        df_notas_fiscais_apuradas, how='left_anti', on='conta_loja'
    )  # somente novas notas fiscais
)

# COMMAND ----------

# Janela que pega os últimos 7 dias
seven_days_in_seconds = 604800  # sete dias em segundos
window_spec_exclusive = (
    Window.partitionBy("conta_loja")
    .orderBy("timestamp")
    .rangeBetween(-seven_days_in_seconds, 0)
)
# Janela que pega o combo de maior prioridade por data
window_combo = Window.partitionBy("conta_loja", "data_nota_fiscal").orderBy(
    F.desc("prioridade")
)

# Janela por conta
window_conta = Window.partitionBy("conta_loja").orderBy("data_nota_fiscal")

# COMMAND ----------

df_compra_dia_rede = (
    df_notas_nao_apuradas.join(df_info_produto, how='inner', on=['ean'])
    .groupBy('conta_loja', 'data_nota_fiscal')
    .agg(
        F.sum('venda_unidades').alias("venda_unidades"),
        F.sum('venda_liquida').alias("venda_liquida"),
        F.collect_set('numero_nota_fiscal').alias('notas_fiscais'),
        F.collect_set('ean').alias('eans'),
    )
)

df_compra_dia_conta_marca = (
    df_notas_nao_apuradas.join(df_info_produto, how='inner', on=['ean'])
    .groupBy('conta_loja', 'marca', 'data_nota_fiscal')
    .agg(
        F.sum('venda_unidades').alias("venda_unidades"),
        F.sum('venda_liquida').alias("venda_liquida"),
    )
    .groupBy("conta_loja", "data_nota_fiscal")
    .agg(
        F.map_from_entries(F.collect_list(F.struct("marca", "venda_unidades"))).alias(
            "venda_do_dia_por_marca"
        ),
        F.map_from_entries(F.collect_list(F.struct("marca", "venda_liquida"))).alias(
            "venda_liquida_do_dia_por_marca"
        ),
    )
)

# COMMAND ----------

df_compra_marca_ultimos_sete = (
    df_notas_nao_apuradas.join(df_info_produto, how='inner', on=['ean'])
    .groupBy('conta_loja', 'marca', 'data_nota_fiscal')
    .agg(
        F.sum('venda_unidades').alias("venda_unidades"),
        F.sum('venda_liquida').alias("venda_liquida"),
    )
    .withColumn(
        "timestamp",
        F.unix_timestamp(F.col("data_nota_fiscal"), "yyyy-MM-dd").cast("long"),
    )
    .withColumn(
        "marca_compra_7_dias",
        F.collect_list(F.struct("marca", "venda_unidades", "venda_liquida")).over(
            window_spec_exclusive
        ),
    )
)


df_soma_compra_conta_marca = (
    df_compra_marca_ultimos_sete.select(
        "conta_loja",
        "data_nota_fiscal",
        "marca",
        F.explode("marca_compra_7_dias").alias("mv"),
    )
    .groupBy("conta_loja", "data_nota_fiscal", "mv.marca")
    .agg(
        F.sum("mv.venda_unidades").alias("soma_vendas"),
        F.sum("mv.venda_liquida").alias("soma_venda_liquida"),
    )
)

df_conta_marca_compra_ultimos_sete = df_soma_compra_conta_marca.groupBy(
    "conta_loja", "data_nota_fiscal"
).agg(
    F.map_from_entries(F.collect_list(F.struct("marca", "soma_vendas"))).alias(
        "vendas_por_marca_7d"
    ),
    F.map_from_entries(F.collect_list(F.struct("marca", "soma_venda_liquida"))).alias(
        "vendas_liq_por_marca_7d"
    ),
)

# COMMAND ----------

df_info_por_marca_consolidado = df_conta_marca_compra_ultimos_sete.join(
    df_compra_dia_conta_marca, on=["conta_loja", "data_nota_fiscal"], how="inner"
).cache()

# COMMAND ----------

prioridades = {"P": 1, "M": 2, "G": 3}

# Criar um DataFrame de prioridades
df_prioridades = spark.createDataFrame(
    list(prioridades.items()), ["combo", "prioridade"]
)
df_info_combo_priorizado = df_info_combo.join(df_prioridades, on="combo", how="left")

df_total_marcas_combo = df_info_combo_priorizado.groupBy("combo").agg(
    F.count("*").alias("qtd_marcas_necessarias")
)

# COMMAND ----------


def detectar_combos_validos(
    df_vendas, df_combos, df_vendas_marca_7d, inicio_apuracao, fim_apuracao
):
    """
    Detecta os combos válidos por conta_loja e data_nota_fiscal com base em:
    1. Todas as marcas do combo estarem presentes em vendas_por_marca_7d
    2. Todas as marcas atingirem a quantidade mínima definida no combo
    3. Selecionar apenas o maior combo por data

    Parâmetros:
        - df_vendas: DataFrame com dados de vendas por rede (conta_loja, data_nota_fiscal)
        - df_combos: DataFrame com definição dos combos (combo, marca, quantidade, prioridade, data_inicio, data_fim)
        - df_vendas_marca_7d: DataFrame com vendas segregdas por marca dos últimos 7 dias e da data de hoje
        - inicio_apuracao: string "yyyy-MM-dd"
        - fim_apuracao: string "yyyy-MM-dd"

    Retorna:
        - DataFrame com: conta_loja, data_nota_fiscal, combo (maior combo válido do dia)
    """

    # Etapa 1: Juntar dados de venda + histórico + combos
    df_joined = (
        df_vendas.join(
            df_vendas_marca_7d, on=["conta_loja", "data_nota_fiscal"], how="inner"
        )
        .crossJoin(df_combos)
        .filter(
            (F.col("data_nota_fiscal") >= F.lit(inicio_apuracao))
            & (F.col("data_nota_fiscal") <= F.lit(fim_apuracao))
        )
    )

    # Etapa 2: Calcular se cada marca do combo está presente e atingiu quantidade
    df_joined = (
        df_joined.withColumn(
            "qtd_vendida_marca", F.col("vendas_por_marca_7d")[F.col("marca")]
        )
        .withColumn("marca_presente", F.col("qtd_vendida_marca").isNotNull())
        .withColumn(
            "atingiu_quantidade", F.col("qtd_vendida_marca") >= F.col("quantidade")
        )
        .withColumn("valida", F.col("marca_presente") & F.col("atingiu_quantidade"))
    )

    # Etapa 3: Validar combos somente se TODAS as marcas forem atingidas
    df_qtd_necessaria = df_combos.groupBy("combo").agg(
        F.count("*").alias("qtd_marcas_combo")
    )

    df_validados = (
        df_joined.groupBy("conta_loja", "data_nota_fiscal", "combo")
        .agg(F.sum(F.col("valida").cast("int")).alias("qtd_marcas_validas"))
        .join(df_qtd_necessaria, on="combo", how="inner")
        .filter(F.col("qtd_marcas_validas") == F.col("qtd_marcas_combo"))
    )

    # Etapa 4: Pegar prioridade e rankear maior combo por dia
    df_combos_prioridade = df_combos.select("combo", "prioridade").dropDuplicates()

    df_rankeado = df_validados.join(df_combos_prioridade, on="combo", how="left")

    window_combo = Window.partitionBy("conta_loja", "data_nota_fiscal").orderBy(
        F.desc("prioridade")
    )

    df_final = (
        df_rankeado.withColumn("rank", F.row_number().over(window_combo))
        .filter(F.col("rank") == 1)
        .select("conta_loja", "data_nota_fiscal", "combo")
        .withColumnRenamed("combo", "combo_formado")
    )

    return df_final


# COMMAND ----------

df_combos_formados = detectar_combos_validos(
    df_vendas=df_compra_dia_rede,
    df_combos=df_info_combo_priorizado,
    df_vendas_marca_7d=df_info_por_marca_consolidado,
    inicio_apuracao=inicio_apuracao,
    fim_apuracao=fim_apuracao,
)

# COMMAND ----------

df_combos_completo = df_combos_formados.join(
    df_info_por_marca_consolidado, on=["conta_loja", "data_nota_fiscal"], how="left"
)

# COMMAND ----------

# Regras do combo quantidade
REGRAS_COMBOS = {}
for row in df_info_combo_priorizado.collect():
    combo = row['combo']
    marca = row['marca']
    quantidade = row['quantidade']
    if combo not in REGRAS_COMBOS:
        REGRAS_COMBOS[combo] = {}
    REGRAS_COMBOS[combo][marca] = quantidade

# Regras do combo Desconto
REGRAS_DESCONTOS = {}
for row in df_info_combo_priorizado.collect():
    combo = row['combo']
    marca = row['marca']
    desconto = row['desconto'] / 100.0
    if combo not in REGRAS_DESCONTOS:
        REGRAS_DESCONTOS[combo] = {}
    REGRAS_DESCONTOS[combo][marca] = desconto

# COMMAND ----------

# schema
schema = T.StructType(
    [
        T.StructField("conta_loja", T.StringType()),
        T.StructField("data_nota_fiscal", T.DateType()),
        T.StructField("combo_formado", T.StringType()),
        T.StructField("valor_cashback", T.FloatType()),
    ]
)


def calcular_cashback(
    combo: str, vendas_liquidas_dict: dict, regras_desconto: dict
) -> float:
    total_cashback = 0.0

    # Obtém os descontos para o combo específico
    descontos_do_combo = regras_desconto.get(combo, {})

    if not descontos_do_combo:
        return 0.0

    # Itera sobre cada marca e seu desconto para o combo atual
    for marca, desconto in descontos_do_combo.items():
        # Pega o valor da venda para a marca, se não existir, considera 0
        valor_venda_marca = vendas_liquidas_dict.get(marca, 0)
        total_cashback += valor_venda_marca * desconto

    return total_cashback


def aplicar_regra_combo_com_cashback(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as regras de formação de combo e calcula o cashback correspondente.
    """
    pdf = pdf.sort_values("data_nota_fiscal").reset_index(drop=True)
    combos_finais = []
    ultima_data_combo = None

    for _, row in pdf.iterrows():
        data = row["data_nota_fiscal"]
        combo = row["combo_formado"]
        conta_loja = row["conta_loja"]

        vendas_dia = row.get("venda_do_dia_por_marca", {}) or {}
        regras_validacao = REGRAS_COMBOS.get(combo, {})

        vendas_liq_7d = row.get("vendas_liq_por_marca_7d", {}) or {}
        vendas_liq_dia = row.get("venda_liquida_do_dia_por_marca", {}) or {}

        # Trata o primeiro combo encontrado
        if ultima_data_combo is None:
            cashback = calcular_cashback(combo, vendas_liq_7d, REGRAS_DESCONTOS)
            combos_finais.append(
                {
                    "data_nota_fiscal": data,
                    "combo_formado": combo,
                    "conta_loja": conta_loja,
                    "valor_cashback": round(cashback, 2),
                }
            )
            ultima_data_combo = data
            continue

        # Regra 1: Respeita janela de 7 dias
        if data >= ultima_data_combo + datetime.timedelta(days=7):
            cashback = calcular_cashback(combo, vendas_liq_7d, REGRAS_DESCONTOS)
            combos_finais.append(
                {
                    "data_nota_fiscal": data,
                    "combo_formado": combo,
                    "conta_loja": conta_loja,
                    "valor_cashback": round(cashback, 2),
                }
            )
            ultima_data_combo = data

        # Regra 2: Validar se venda do próprio dia fecha o combo caso intervalo for menor que sete dias
        elif all(
            vendas_dia.get(marca, 0) >= qtd for marca, qtd in regras_validacao.items()
        ):
            cashback = calcular_cashback(combo, vendas_liq_dia, REGRAS_DESCONTOS)
            combos_finais.append(
                {
                    "data_nota_fiscal": data,
                    "combo_formado": combo,
                    "conta_loja": conta_loja,
                    "valor_cashback": round(cashback, 2),
                }
            )
            ultima_data_combo = data

    if not combos_finais:
        return pd.DataFrame(
            columns=[
                "data_nota_fiscal",
                "combo_formado",
                "conta_loja",
                "valor_cashback",
            ]
        )

    result_df = pd.DataFrame(combos_finais)
    result_df["data_nota_fiscal"] = pd.to_datetime(result_df["data_nota_fiscal"])

    return result_df


# COMMAND ----------

df_combo_validados = df_combos_completo.groupBy("conta_loja").applyInPandas(
    aplicar_regra_combo_com_cashback, schema=schema
)

# COMMAND ----------

df_apuracao_combo = (
    df_compra_dia_rede.join(
        df_combo_validados, how='inner', on=['conta_loja', 'data_nota_fiscal']
    )
    .withColumn("tipo_campanha", F.lit(f'combo - {inicio_apuracao}/{fim_apuracao}'))
    .withColumn('data_combo_formado', F.col("data_nota_fiscal"))
    .withColumn("data_execucao_apuracao", F.lit(F.current_date()))
    .select(
        "conta_loja",
        "data_combo_formado",
        "combo_formado",
        "valor_cashback",
        "eans",
        "notas_fiscais",
        "data_execucao_apuracao",
        "tipo_campanha",
    )
)

# COMMAND ----------

df_apuracao_combo.write.mode("append").saveAsTable(apuracao_combo_table)
