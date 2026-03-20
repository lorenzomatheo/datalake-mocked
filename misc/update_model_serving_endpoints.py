# Databricks notebook source
dbutils.widgets.dropdown("stage", "dev", ["dev", "staging", "production"])
dbutils.widgets.text("enable_scale_to_zero", "true")

# COMMAND ----------

# MAGIC %pip install mlflow==2.20.2 mlflow[databricks]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import timedelta

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)
from mlflow.tracking import MlflowClient

stage = dbutils.widgets.get("stage")

if stage == 'production':
    WORKLOAD_SIZE = "Medium"
else:
    WORKLOAD_SIZE = "Small"

mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")
DATABRICKS_TOKEN = dbutils.secrets.get(
    scope='databricks', key='DATABRICKS_ACCESS_TOKEN'
)
DATABRICKS_WORKSPACE_URL = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

ENABLE_SCALE_TO_ZERO = dbutils.widgets.get("enable_scale_to_zero")
SCALE_TO_ZERO = ENABLE_SCALE_TO_ZERO.lower() == "true"

# CHAT MODEL SERVING
CHAT_ENDPOINT_NAME = f"chat_milvus_{stage}"
CHAT_MODEL_NAME = f"{stage}.refined.chat_milvus"

# VANNA MODEL SERVING
VANNA_MODEL_NAME = f"{stage}.refined.vanna_text_to_sql"
VANNA_ENDPOINT_NAME = f"vanna_{stage}"

# COMMAND ----------


def update_endpoint(MODEL_NAME, ENDPOINT_NAME):
    client = MlflowClient()
    filter_string = f"name='{MODEL_NAME}'"
    results = client.search_model_versions(filter_string)
    latest_model_version = results[0].version

    w = WorkspaceClient(host=DATABRICKS_WORKSPACE_URL, token=DATABRICKS_TOKEN)

    existing_endpoint = next(
        (e for e in w.serving_endpoints.list() if e.name == ENDPOINT_NAME), None
    )

    environment_vars = {
        "DATABRICKS_HOST": DATABRICKS_WORKSPACE_URL,
        "DATABRICKS_TOKEN": "{{secrets/databricks/DATABRICKS_ACCESS_TOKEN}}",
    }

    if existing_endpoint:
        detailed_endpoint = w.serving_endpoints.get(ENDPOINT_NAME)

        if detailed_endpoint.config:
            served_objects = None
            # Check for served_entities (new) or served_models (deprecated)
            if detailed_endpoint.config.served_entities:
                served_objects = detailed_endpoint.config.served_entities
            elif detailed_endpoint.config.served_models:
                served_objects = detailed_endpoint.config.served_models

            if served_objects:
                first_object = served_objects[0]
                current_env_vars = first_object.environment_vars

                if current_env_vars:
                    environment_vars.update(current_env_vars)

    endpoint_config = EndpointCoreConfigInput(
        name=ENDPOINT_NAME,
        served_entities=[
            ServedEntityInput(
                entity_name=MODEL_NAME,
                entity_version=latest_model_version,
                workload_size=WORKLOAD_SIZE,
                scale_to_zero_enabled=SCALE_TO_ZERO,
                environment_vars=environment_vars,
            )
        ],
    )

    serving_endpoint_url = f"{DATABRICKS_WORKSPACE_URL}/ml/endpoints/{ENDPOINT_NAME}"
    if existing_endpoint is None:
        print(
            f"Creating the endpoint {serving_endpoint_url}, this will take a few minutes to package and deploy the endpoint..."
        )
        w.serving_endpoints.create_and_wait(
            name=ENDPOINT_NAME,
            config=endpoint_config,
            timeout=timedelta(minutes=30),
        )
    else:
        print(
            f"Updating the endpoint {serving_endpoint_url} to version {latest_model_version}"
            ", this will take a few minutes to package and deploy the endpoint..."
        )
        w.serving_endpoints.update_config_and_wait(
            served_entities=endpoint_config.served_entities,
            name=ENDPOINT_NAME,
            timeout=timedelta(minutes=30),
        )

    displayHTML(
        f'Your Model Endpoint Serving is now available. Open the <a href="/ml/endpoints/{ENDPOINT_NAME}">Model Serving Endpoint page</a> for more details.'
    )
    client.set_registered_model_alias(
        name=MODEL_NAME, alias=f"{ENDPOINT_NAME}", version=latest_model_version
    )


# COMMAND ----------

update_endpoint(CHAT_MODEL_NAME, CHAT_ENDPOINT_NAME)

# COMMAND ----------

# update_endpoint(VANNA_MODEL_NAME, VANNA_ENDPOINT_NAME)
