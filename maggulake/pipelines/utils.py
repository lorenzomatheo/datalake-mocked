from datetime import datetime
from typing import Dict, List, Union

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T


def format_column_value(value: Union[None, str, list]) -> str:
    """
    Formata valores de colunas para inclusão em prompts.
    Trata: None -> "N/A", Lista -> string join, Outros -> str().
    """
    if value is None:
        return "N/A"
    if isinstance(value, list):
        if not value:
            return "N/A"
        return ", ".join(str(v) for v in value)
    return str(value)


def validate_llm_results(results: List[Dict], schema_fields: List[str]) -> List[Row]:
    """Valida e converte dicionários do LLM em Rows, tratando nulls e timestamp.
    Filtra automaticamente rows onde 'ean' é None."""
    validated: List[Row] = []
    for res in results:
        if not isinstance(res, dict):
            continue

        complete_res = {}
        for field in schema_fields:
            val = res.get(field, None)
            if isinstance(val, str) and val.strip().lower() == "null":
                val = None
            complete_res[field] = val

        if "ean" in complete_res and complete_res["ean"] is None:
            continue

        complete_res["atualizado_em"] = datetime.now()
        validated.append(Row(**complete_res))
    return validated


def save_enrichment_batch(
    spark: SparkSession,
    rows: List[Row],
    table_name: str,
    schema: T.StructType,
    merge_keys: Union[List[str], None] = None,
) -> int:
    """Escreve Rows na tabela Delta usando Merge. Retorna contagem de salvos."""
    if merge_keys is None:
        merge_keys = ["ean"]

    if not rows:
        return 0

    df_results = spark.createDataFrame(rows, schema)
    df_results = df_results.dropDuplicates(merge_keys)

    delta_table = DeltaTable.forName(spark, table_name)
    condition_str = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
    schema_fields = [f.name for f in schema.fields]

    delta_table.alias("target").merge(
        source=df_results.alias("source"), condition=condition_str
    ).whenMatchedUpdate(
        set={col: f"source.{col}" for col in schema_fields if col not in merge_keys}
    ).whenNotMatchedInsert(
        values={col: f"source.{col}" for col in schema_fields}
    ).execute()

    return len(rows)
