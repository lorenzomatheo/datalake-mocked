import json
import os
import warnings
from typing import Optional, Tuple

import langsmith
import vertexai
from google import genai
from google.genai import errors
from google.genai.types import CreateCachedContentConfig
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

_vertex_config_cache = {}


def setup_vertex_ai_credentials(
    spark: SparkSession, location: str = "us-central1"
) -> Tuple[str, str]:
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and _vertex_config_cache:
        return _vertex_config_cache["project_id"], _vertex_config_cache["location"]

    dbutils = DBUtils(spark)
    google_application_credentials = json.loads(
        dbutils.secrets.get(scope="databricks", key="GOOGLE_VERTEX_CREDENTIALS")
    )

    credentials_path = "/dbfs/tmp/gcp_credentials.json"
    with open(credentials_path, "w") as file:
        json.dump(google_application_credentials, file)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    project_id = google_application_credentials["project_id"]

    vertexai.init(project=project_id, location=location)

    _vertex_config_cache["project_id"] = project_id
    _vertex_config_cache["location"] = location

    return project_id, location


def setup_langsmith(
    dbutils,
    spark: SparkSession,
    project: str | None = None,
    stage: str | None = None,
) -> None:
    """Configura variaveis de ambiente para habilitar tracing via LangSmith.

    O nome do projeto no LangSmith é derivado automaticamente do appName da
    SparkSession (definido em DatabricksEnvironmentBuilder.build), mas pode
    ser sobrescrito via project.
    """
    api_key = dbutils.secrets.get(scope="llms", key="LANGSMITH_API_KEY")
    if project is None:
        # spark.sparkContext.appName e sempre "Databricks Shell" em clusters interativos
        # porque getOrCreate() nao altera o appName de uma sessao ja existente.
        # Lemos de maggu.app.name, setado por DatabricksEnvironmentBuilder ao criar o env.
        project = spark.conf.get("maggu.app.name", spark.sparkContext.appName)
    full_project = f"{project}-{stage}" if stage else project

    # Nomes antigos (langsmith < 0.2 / langchain-core)
    os.environ["LANGCHAIN_TRACING_V2"] = "true"
    os.environ["LANGCHAIN_API_KEY"] = api_key
    os.environ["LANGCHAIN_PROJECT"] = full_project

    # Nomes novos (langsmith >= 0.2)
    os.environ["LANGSMITH_TRACING"] = "true"
    os.environ["LANGSMITH_API_KEY"] = api_key
    os.environ["LANGSMITH_PROJECT"] = full_project

    print(
        f"LangSmith tracing habilitado | projeto: '{full_project}' | "
        f"langsmith=={langsmith.__version__}"
    )


def create_vertex_context_cache(
    model: str,
    system_instruction: str,
    project_id: str,
    location: str = "us-central1",
    ttl: int = 3600,
) -> Optional[str]:
    client = genai.Client(vertexai=True, project=project_id, location=location)
    try:
        cache = client.caches.create(
            model=model,
            config=CreateCachedContentConfig(
                system_instruction=system_instruction,
                ttl=f"{ttl}s",
            ),
        )
    except errors.APIError as e:
        if "minimum token count" in str(e).lower():
            return None
        raise

    last_name = cache.name.split("/")[-1]

    return last_name


def get_location_by_model(model_name: str) -> str:
    """Retorna a regiao do Google Cloud adequada para o modelo especificado."""
    if model_name.startswith("gemini"):
        return "us-central1"

    warnings.warn(
        f"Modelo desconhecido '{model_name}'. Usando regiao padrao 'us-central1'."
    )
    return "us-central1"
