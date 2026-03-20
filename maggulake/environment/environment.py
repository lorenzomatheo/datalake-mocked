# pylint: disable=import-outside-toplevel
# Esses imports estao la pra ser lazy e
# so gastar tempo importando quando precisar

from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from typing import Iterator

from maggulake.tables import BaseTable

from .configs import configs
from .settings import Settings, Stage
from .tables import BigqueryGold, BigqueryView, BigqueryViewPbi, CopilotTable, Table

DEFAULT_SCHEMA = 'refined'


@dataclass
class Environment:
    settings: Settings
    session_name: str
    spark_config: dict = None

    def full_s3_path(self, key):
        return f's3://{self.settings.bucket}/{key}'

    @cached_property
    def spark(self):
        from pyspark.context import SparkConf
        from pyspark.sql import SparkSession

        spark_config = self.spark_config or {}
        config = SparkConf().setAll(
            [("spark.sql.caseSensitive", "true"), *spark_config.items()]
        )

        spark = (
            SparkSession.builder.appName(f"{self.session_name}_{self.settings.stage}")
            .config(conf=config)
            .getOrCreate()
        )
        spark.conf.set("maggu.app.name", self.session_name)

        spark.sql(f"USE CATALOG {self.settings.catalog}")
        spark.sql(f"USE SCHEMA {DEFAULT_SCHEMA}")

        return spark

    def table(self, table: Table | CopilotTable | Enum):
        if isinstance(table.value, type) and issubclass(table.value, BaseTable):
            return table.value(self.spark, stage=self.settings.stage)
        if isinstance(table, Table):
            return self.spark.read.table(table.value)
        if isinstance(table, CopilotTable):
            return self.spark.read.table(f"{self.get_bigquery_schema()}.{table.value}")
        if isinstance(table, BigqueryView):
            return self.spark.read.table(
                f"{self.get_bigquery_views_schema()}.{table.value}"
            )
        if isinstance(table, BigqueryGold):
            return self.spark.read.table(
                f"{self.get_bigquery_schema_gold()}.{table.value}"
            )
        if isinstance(table, BigqueryViewPbi):
            return self.spark.read.table(
                f"{self.get_bigquery_views_pbi_schema()}.{table.value}"
            )
        raise RuntimeError("Tabela deve ser do Enum")

    def create_table_if_not_exists(self, table: Table, schema: str) -> None:
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table.value} (
            {schema}
        )
        USING DELTA
        """)

    def get_bigquery_schema(self):
        match self.settings.stage:
            case Stage.PRODUCTION:
                return "bigquery.postgres_public"
            case Stage.STAGING:
                return "bigquery.postgres_staging_public"

    def get_bigquery_views_schema(self):
        match self.settings.stage:
            case Stage.PRODUCTION:
                return "bigquery.databricks_views_production"
            case Stage.STAGING:
                return "bigquery.databricks_views_staging"

    def get_bigquery_schema_gold(self):
        # o mesmo para prod e staging
        return "bigquery.metricas_produto_maggu"

    def get_bigquery_views_pbi_schema(self):
        # o mesmo para prod e staging
        return "bigquery.views_pbi"

    @cached_property
    def bigquery_adapter(self):
        from maggulake.io.bigquery import BigQueryAdapter

        return BigQueryAdapter(
            spark=self.spark,
            schema=self.get_bigquery_schema(),
        )

    @cached_property
    def postgres_adapter(self):
        from maggulake.io.postgres import PostgresAdapter

        return PostgresAdapter(
            stage=self.settings.name_short,
            user=self.settings.postgres_user,
            password=self.settings.postgres_password,
            host=self.settings.postgres_host,
            database=self.settings.postgres_database,
            port=self.settings.postgres_port,
        )

    @cached_property
    def postgres_replica_adapter(self):
        from maggulake.io.postgres import PostgresAdapter

        if self.settings.stage == Stage.PRODUCTION:
            return PostgresAdapter(
                stage=self.settings.name_short,
                user=self.settings.postgres_user,
                password=self.settings.postgres_password,
                host=self.settings.postgres_replica_host,
                database=self.settings.postgres_database,
                port=self.settings.postgres_port,
            )

        return self.postgres_adapter

    @cached_property
    def openai(self):
        from openai import OpenAI

        return OpenAI(
            organization=self.settings.openai_organization,
            api_key=self.settings.openai_api_key,
        )

    @cached_property
    def gemini(self):
        from google import genai

        return genai.Client(api_key=self.settings.gemini_api_key)

    @cached_property
    def vector_search(self):
        from .vector_search import VectorSearch

        return VectorSearch(settings=self.settings, openai=self.openai)

    @cached_property
    def bigdatacorp_client(self):
        from maggulake.integrations.bigdatacorp import BigDataCorpClient

        return BigDataCorpClient(
            self.settings.bigdatacorp_address, self.settings.bigdatacorp_token
        )

    @cached_property
    def bluesoft_client(self):
        from maggulake.integrations.bluesoft import BluesoftClient

        return BluesoftClient(
            self.settings.bluesoft_address, self.settings.bluesoft_token
        )

    @cached_property
    def portal_obm_client(self):
        from maggulake.integrations.portal_obm import PortalObmClient

        return PortalObmClient(
            self.settings.portal_obm_address, self.settings.portal_obm_token
        )

    def get_embeddings_udf(self, model_name="text-embedding-3-large", dimensions=1536):
        import pandas as pd
        import pyspark.sql.functions as F

        self.spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 500)
        openai_api_key = self.settings.openai_api_key

        @F.pandas_udf("array<double>")
        def embeddings_udf(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
            import time

            from openai import OpenAI, RateLimitError

            client = OpenAI(api_key=openai_api_key)

            for texts in iterator:
                max_retries = 3
                retry_count = 0

                while retry_count <= max_retries:
                    try:
                        response = client.embeddings.create(
                            input=texts, model=model_name, dimensions=dimensions
                        )
                        time.sleep(0.2)
                        yield pd.Series([e.embedding for e in response.data])
                        break
                    except RateLimitError:
                        retry_count += 1
                        if retry_count > max_retries:
                            raise

                        wait_time = (2**retry_count) * 2
                        time.sleep(wait_time)

        return embeddings_udf


class DatabricksEnvironmentBuilder:
    @staticmethod
    def build(
        session_name: str,
        dbutils,
        spark_config: dict = None,
        # funciona pra text e dropdown (o default é o primeiro da lista)
        widgets: dict[str | list[str]] = None,
    ) -> Environment:
        dbutils.widgets.dropdown(
            # Depois podemos mudar pra stage, mas ia dar muito conflito
            "environment",
            Stage.STAGING.value,
            [e.value for e in Stage],
        )
        dbutils.widgets.dropdown(
            "debug",
            "false",
            ["true", "false"],
        )

        widgets = widgets or {}

        for nome, valor in widgets.items():
            if isinstance(valor, str):
                dbutils.widgets.text(nome, valor)
            if isinstance(valor, list):
                dbutils.widgets.dropdown(nome, valor[0], valor)

        stage = Stage(dbutils.widgets.get("environment"))

        return Environment(
            session_name=session_name,
            spark_config=spark_config,
            settings=DatabricksEnvironmentBuilder.get_settings(stage, dbutils),
        )

    @staticmethod
    def get_settings(stage: Stage, dbutils):
        stage_configs = configs[stage.value]
        STAGE = stage_configs["name"].upper()
        STAGE_SHORT = stage_configs["name_short"].upper()

        return Settings.create(
            stage=stage,
            openai_organization=dbutils.secrets.get(
                scope="openai", key="OPENAI_ORGANIZATION"
            ),
            openai_api_key=dbutils.secrets.get(
                scope="openai", key=f"OPENAI_API_KEY_{STAGE_SHORT}"
            ),
            milvus_uri=dbutils.secrets.get(
                scope="databricks", key=f"MILVUS_URI_{STAGE}"
            ),
            milvus_token=dbutils.secrets.get(
                scope="databricks", key=f"MILVUS_TOKEN_{STAGE}"
            ),
            data_api_key=dbutils.secrets.get(
                scope="data_api", key=f"DATA_API_KEY_{STAGE}"
            ),
            postgres_password=dbutils.secrets.get(
                scope="postgres", key="POSTGRES_PASSWORD"
            ),
            gemini_api_key=dbutils.secrets.get(scope="llms", key="GEMINI_API_KEY"),
            bigdatacorp_token=dbutils.secrets.get(
                scope="databricks", key="BIGDATACORP_TOKEN"
            ),
            bluesoft_token=dbutils.secrets.get(scope="bluesoft", key="BLUESOFT_TOKEN"),
            portal_obm_token=dbutils.secrets.get(
                scope="databricks", key="PORTAL_OBM_TOKEN"
            ),
        )
