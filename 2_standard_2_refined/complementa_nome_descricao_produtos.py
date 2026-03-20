# Databricks notebook source
# MAGIC %pip install requests scikit-learn

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pyspark.sql.functions as F

from maggulake.environment import DatabricksEnvironmentBuilder, Table
from maggulake.pipelines.short_products.produtos_texto_curto import (
    ProdutosTextoCurtoConfig,
    enriquece_nome_descricao_externos,
    prepara_produtos_texto_curto,
    salva_produtos_texto_curto,
    salva_produtos_texto_externos,
)
from maggulake.utils.time import agora_em_sao_paulo_str

# COMMAND ----------

env = DatabricksEnvironmentBuilder.build(
    "complementa_nome_descricao_produtos",
    dbutils,
    spark_config={"spark.sql.caseSensitive": "true"},
    widgets={
        "max_products": "10000",
        "min_text_len": "10",
    },
)

# COMMAND ----------

DEBUG = dbutils.widgets.get("debug") == "true"
MAX_PRODUCTS = int(dbutils.widgets.get("max_products"))
MIN_TEXT_LEN = int(dbutils.widgets.get("min_text_len"))
spark = env.spark

config_produtos_texto_curto = ProdutosTextoCurtoConfig(env=env)

# COMMAND ----------

produtos = env.table(Table.produtos_standard).cache()

# TODO: Isso parece um join que todo enriquecimento teria que fazer. E pode ser que existam outros. Acho que poderia ajudar se houvesse ou uma tabela nesse estado, ou uma função helper que já devolva a tabela pronta.
eh_medicamento_completo = env.table(Table.coluna_eh_medicamento_completo).select(
    "ean", F.col("eh_medicamento").alias("eh_medicamento_consolidado")
)

produtos = produtos.join(eh_medicamento_completo, on=["ean"], how="left")

if DEBUG:
    print(f"{agora_em_sao_paulo_str()} - Total produtos: {produtos.count()}")

# COMMAND ----------

# Somente nao medicamentos.
produtos = produtos.filter(
    F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento"))
    == F.lit(False)
)

# EAN e necessario para buscar em fontes externas.
produtos = produtos.dropna(subset=["ean"])

# COMMAND ----------

produtos_texto_curto = prepara_produtos_texto_curto(
    produtos,
    MIN_TEXT_LEN,
).cache()

# COMMAND ----------

if produtos_texto_curto.isEmpty():
    dbutils.notebook.exit("Sem produtos com nome/descricao curtos.")

# COMMAND ----------

salva_produtos_texto_curto(
    produtos_texto_curto,
    config_produtos_texto_curto,
)

# COMMAND ----------

total_original = produtos_texto_curto.count()
produtos_para_buscar = produtos_texto_curto

if MAX_PRODUCTS and total_original > MAX_PRODUCTS:
    print(
        f"{agora_em_sao_paulo_str()} - Limitando para {MAX_PRODUCTS} de {total_original} produtos"
    )
    produtos_para_buscar = produtos_para_buscar.limit(MAX_PRODUCTS)

# COMMAND ----------

produtos_texto_externos = enriquece_nome_descricao_externos(
    produtos_para_buscar.select("ean").distinct(),
    config_produtos_texto_curto,
).cache()

salva_produtos_texto_externos(
    produtos_texto_externos,
    config_produtos_texto_curto,
)

# TODO: Acho que esse eh um ponto de atencao... Se um EAN nao foi encontrado, precisamos deixar esse resultado registrado e parar de consultar o mesmo EAN em todas as execucoes. @lorenzomatheo por favor adiciona um TODO no seu codigo para podermos ajustar isso depois

if DEBUG:
    print(
        f"{agora_em_sao_paulo_str()} - Produtos externos salvos: {produtos_texto_externos.count()}"
    )
    produtos_texto_externos.limit(100).display()
