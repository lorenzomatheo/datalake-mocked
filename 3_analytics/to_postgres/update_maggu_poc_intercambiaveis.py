# Databricks notebook source
# MAGIC %md # Atualiza intercambiaveis no postgress do copilot
# MAGIC
# MAGIC Racional:
# MAGIC - Queremos enviar a tabela intercambiaveis do databricks para o copilot
# MAGIC - Use esse notebook para criar um objeto na tabela "ImportDatabricks" do copilot.
# MAGIC - Esse notebook é, portanto, equivalente a utilizar o portal admin do django para criar um objeto na tabela SyncDatabricks
# MAGIC - Após rodar o notebook, você deve executar a migração dos dados atraveś do container kubernetes

# COMMAND ----------


import os
import urllib.parse

import pyspark.sql.functions as F
import requests

from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.environment import DatabricksEnvironmentBuilder, Table

env = DatabricksEnvironmentBuilder.build(
    "update_maggu_poc_intercambiaveis",
    dbutils,
)

# COMMAND ----------

stage = env.settings.name_short
spark = env.spark
print("Stage selecionado: ", stage)

# COMMAND ----------

url_by_stage = {
    "dev": "https://staging.copilot.maggu.ai",
    "prod": "https://copilot.maggu.ai",
}
maggu_copilot_url = url_by_stage.get(stage, url_by_stage["dev"])

secrets_scope_by_stage = {
    "dev": "staging.copilot.maggu.ai",
    "prod": "copilot.maggu.ai",
}
secrets_scope = secrets_scope_by_stage.get(stage, secrets_scope_by_stage["dev"])
token = dbutils.secrets.get(scope=secrets_scope, key="COPILOT_MAGGU_API_TOKEN")

s3_jsons_folder = env.full_s3_path("5-sharing-layer/copilot.maggu.ai/intercambiaveis")

# COMMAND ----------

print("Pasta com produtos intercambiaveis (s3_jsons_folder): ", s3_jsons_folder)

# COMMAND ----------

# MAGIC %md ### Grava os jsons dos intercambiaveis

# COMMAND ----------

# Salva na camada "5-sharing-layer"
# Alterando a tabela para ficar no formato da tabela do copilot
# TODO: Mudar o formato da tabela do copilot para ficar como esta
intercambiaveis = (
    env.table(Table.intercambiaveis)
    .withColumn("tipo_referencia", F.lit(TipoMedicamento.REFERENCIA.value))
    .withColumnRenamed("ean_intercambiavel", "ean_similar")
    .withColumnRenamed("marca_intercambiavel", "marca_similar")
    .withColumnRenamed("tipo_intercambiavel", "tipo_similar")
)

intercambiaveis.write.mode("overwrite").json(s3_jsons_folder)

print("total de produtos na tabela de intercambiaveis: ", intercambiaveis.count())

# COMMAND ----------

# MAGIC %md ### Inicia o processamento dos dados no `copilot.maggu.ai`

# COMMAND ----------


s3_intercambiaveis_folder = os.path.join(s3_jsons_folder)

payload = {
    "tenant": "maggu",
    "imports": [
        {"modelType": "INTERCAMBIAVEIS", "s3JsonsFolder": s3_intercambiaveis_folder},
    ],
}
headers = {
    "Authorization": f"Bearer {token}",
}

url = urllib.parse.urljoin(maggu_copilot_url, "data-import/databricks-import")
requests.post(url, json=payload, headers=headers)

# COMMAND ----------

# MAGIC %md
# MAGIC * A célula acima deve ter respondido `<Response [204]>`, que é sucesso
# MAGIC * Para conferir se o objeto foi criado corretamente, acesse: `{maggu_copilot_url}/admin/data_import/databricksimport/`
# MAGIC * Lembre-se de realizar a importação dos dados pelo kubernets (utilize o pods intercambiaveis)
