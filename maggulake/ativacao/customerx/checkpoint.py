"""Módulo para gerenciamento de checkpoints de sincronização CustomerX."""

from datetime import datetime
from typing import Literal

import pytz
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

StatusType = Literal["success", "failed", "running"]

# TODO: esses 2 funções `get_last_sync` e `update_checkpoint` poderiam usar o Environment do maggulake.environment


def get_last_sync(
    spark: SparkSession,
    catalog: str,
    notebook_name: str,
) -> datetime | None:
    """Obtém o timestamp da última sincronização bem-sucedida.

    Args:
        spark: SparkSession ativa
        catalog: Nome do catálogo (staging ou production)
        notebook_name: Nome do notebook que executou a sincronização

    Returns:
        Timestamp da última sincronização bem-sucedida ou None se não houver histórico
    """
    table_name = f"{catalog}.raw.customerx_sync_checkpoint"

    # Verificar se a tabela existe
    if not spark.catalog.tableExists(table_name):
        print(f"⚠️  Tabela {table_name} não existe. Primeira execução.")
        return None

    # Buscar última sincronização bem-sucedida
    df = spark.sql(
        f"""
        SELECT last_sync_at
        FROM {table_name}
        WHERE notebook_name = '{notebook_name}'
          AND status = 'success'
        ORDER BY last_sync_at DESC
        LIMIT 1
        """
    )

    rows = df.collect()
    if not rows:
        print(
            f"ℹ️  Nenhum checkpoint encontrado para '{notebook_name}'. Primeira execução."
        )
        return None

    last_sync = rows[0]["last_sync_at"]
    print(f"📅 Última sincronização bem-sucedida: {last_sync}")
    return last_sync


def update_checkpoint(
    spark: SparkSession,
    catalog: str,
    notebook_name: str,
    status: StatusType,
    records_processed: int = 0,
    error_message: str | None = None,
) -> None:
    """Atualiza o checkpoint de sincronização.

    Args:
        spark: SparkSession ativa
        catalog: Nome do catálogo (staging ou production)
        notebook_name: Nome do notebook que executou a sincronização
        status: Status da execução (success, failed, running)
        records_processed: Quantidade de registros processados
        error_message: Mensagem de erro se status = failed
    """

    table_name = f"{catalog}.raw.customerx_sync_checkpoint"
    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    # Schema da tabela
    # TODO: mover para a pasta de schemas. Não fiz isso ainda pois quero ter ctz que isso vai pra frente
    schema = StructType(
        [
            StructField("notebook_name", StringType(), False),
            StructField("last_sync_at", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("error_message", StringType(), True),
            StructField("updated_at", TimestampType(), False),
        ]
    )

    # Criar DataFrame com novo registro
    new_record = spark.createDataFrame(
        [
            (
                notebook_name,
                now,
                status,
                records_processed,
                error_message,
                now,
            )
        ],
        schema=schema,
    )

    # Verificar se a tabela existe
    # TODO: dava pra usar o env.create_table_if_not_exists aqui
    if not spark.catalog.tableExists(table_name):
        # Criar tabela se não existir
        new_record.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"✅ Tabela {table_name} criada e checkpoint registrado")
        return

    # Fazer upsert usando merge
    delta_table = DeltaTable.forName(spark, table_name)

    # Se for status "running", apenas insere (para não sobrescrever o último sucesso)
    # Se for "success" ou "failed", faz upsert (atualiza se existe, insere se não)
    if status == "running":
        new_record.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"📝 Checkpoint 'running' registrado para '{notebook_name}'")
    else:
        delta_table.alias("target").merge(
            new_record.alias("source"),
            "target.notebook_name = source.notebook_name",
        ).whenMatchedUpdate(
            set={
                "last_sync_at": F.col("source.last_sync_at"),
                "status": F.col("source.status"),
                "records_processed": F.col("source.records_processed"),
                "error_message": F.col("source.error_message"),
                "updated_at": F.col("source.updated_at"),
            }
        ).whenNotMatchedInsertAll().execute()

        status_emoji = "✅" if status == "success" else "❌"
        print(
            f"{status_emoji} Checkpoint atualizado: {notebook_name} - "
            f"{status} ({records_processed} registros)"
        )
