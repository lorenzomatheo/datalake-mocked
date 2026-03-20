"""Reporting utilities for CustomerX sync operations."""

from dataclasses import dataclass
from datetime import datetime

import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Standard schema for sync summary reports
SYNC_SUMMARY_SCHEMA = StructType(
    [
        StructField("Métrica", StringType(), nullable=False),
        StructField("Valor", IntegerType(), nullable=False),
    ]
)

# Schema for error tracking reports
ERROR_SUMMARY_SCHEMA = StructType(
    [
        StructField("ID", StringType(), nullable=False),
        StructField("Tipo", StringType(), nullable=False),
        StructField("Mensagem", StringType(), nullable=False),
    ]
)

# Schema for CustomerX integration error logging
# TODO: Clean errors older than 1 year periodically
CX_INTEGRATION_ERROR_SCHEMA = StructType(
    [
        StructField("notebook_name", StringType(), nullable=False),
        StructField("operation_type", StringType(), nullable=False),
        StructField("entity_id", StringType(), nullable=False),
        StructField("entity_name", StringType(), nullable=True),
        StructField("error_message", StringType(), nullable=False),
        StructField("error_timestamp", TimestampType(), nullable=False),
        StructField("catalog", StringType(), nullable=False),
    ]
)


def create_summary_df(spark: SparkSession, data: list, schema: StructType):
    """
    Create a summary DataFrame for display.

    Args:
        spark: SparkSession instance
        data: List of tuples/dicts matching the schema
        schema: StructType defining the DataFrame schema

    Returns:
        DataFrame with the summary data
    """
    return spark.createDataFrame(data, schema)


@dataclass
class CXIntegrationError:
    """Dataclass para acumular erros de integração CustomerX."""

    notebook_name: str
    operation_type: str
    entity_id: str
    entity_name: str | None
    error_message: str
    error_timestamp: datetime
    catalog: str


class ErrorAccumulator:
    """
    Acumula erros durante processamento e salva em batch ao final.

    Uso:
        error_acc = ErrorAccumulator(spark, catalog, "notebook_name")
        try:
            # Loop de processamento
            for item in items:
                try:
                    # processar item
                    pass
                except Exception as e:
                    error_acc.add_error(
                        operation_type="criar_cliente",
                        entity_id=item.id,
                        entity_name=item.name,
                        error_message=str(e)
                    )
        finally:
            error_acc.save_errors()
    """

    def __init__(self, spark: SparkSession, catalog: str, notebook_name: str):
        self.spark = spark
        self.catalog = catalog
        self.notebook_name = notebook_name
        self.errors: list[CXIntegrationError] = []
        self.table_name = f"{catalog}.analytics.cx_integration_errors"

    def add_error(
        self,
        operation_type: str,
        entity_id: str,
        error_message: str,
        entity_name: str | None = None,
    ) -> None:
        """Adiciona erro à lista de erros acumulados."""
        self.errors.append(
            CXIntegrationError(
                notebook_name=self.notebook_name,
                operation_type=operation_type,
                entity_id=entity_id,
                entity_name=entity_name,
                error_message=error_message,
                error_timestamp=datetime.now(tz=pytz.timezone("America/Sao_Paulo")),
                catalog=self.catalog,
            )
        )

    def save_errors(self) -> None:
        """
        Salva todos os erros acumulados na tabela Delta em um único append.
        TODO: Clean errors older than 1 year periodically
        """
        if not self.errors:
            return

        error_tuples = [
            (
                err.notebook_name,
                err.operation_type,
                err.entity_id,
                err.entity_name,
                err.error_message,
                err.error_timestamp,
                err.catalog,
            )
            for err in self.errors
        ]

        df_errors = self.spark.createDataFrame(
            error_tuples, schema=CX_INTEGRATION_ERROR_SCHEMA
        )

        df_errors.write.format("delta").mode("append").saveAsTable(self.table_name)
