# Databricks notebook source
# MAGIC %pip install agno
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os

from agno.agent import Agent
from agno.models.google import Gemini
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("maggu_vertex_model").getOrCreate()

GOOGLE_APPLICATION_CREDENTIALS = json.loads(
    dbutils.secrets.get(scope="databricks", key="GOOGLE_VERTEX_CREDENTIALS")
)

credentials_path = "/dbfs/tmp/gcp_credentials.json"
with open(credentials_path, "w") as f:
    json.dump(GOOGLE_APPLICATION_CREDENTIALS, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# COMMAND ----------

PROJECT_ID = "1092371128279"
LOCATION = "us-central1"
MODEL_ID = "projects/1092371128279/locations/us-central1/endpoints/9189310266137903104"

# COMMAND ----------

maggu_agent = Agent(
    model=Gemini(
        id=MODEL_ID,
        vertexai=True,
        project_id=PROJECT_ID,
        location=LOCATION,
        thinking_budget=0,  # desliga o “thinking”
        include_thoughts=False,  # não retorna pensamentos
    ),
    markdown=True,
    reasoning=False,  # desliga o raciocínio Agno
)


maggu_agent.print_response("Olá, qual o seu nome?")

# COMMAND ----------

maggu_agent.print_response("Qual a posologia do Doril?")
