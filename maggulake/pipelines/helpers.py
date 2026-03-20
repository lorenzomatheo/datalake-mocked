import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from maggulake.environment.environment import Environment
from maggulake.environment.tables import Table


def get_produtos_standard_com_eh_medicamento(
    env: Environment,
    eh_medicamento: bool,
) -> DataFrame:
    """Lê produtos_standard, cruza com eh_medicamento_completo e filtra por tipo.

    Args:
        env: Ambiente Databricks.
        eh_medicamento: True para manter medicamentos, False para não-medicamentos.
    """
    produtos = env.table(Table.produtos_standard)

    tabela = env.table(Table.coluna_eh_medicamento_completo)
    window_spec = Window.partitionBy("ean").orderBy(F.desc("atualizado_em"))

    eh_medicamento_recente = (
        tabela.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .select("ean", F.col("eh_medicamento").alias("eh_medicamento_consolidado"))
    )

    return produtos.join(eh_medicamento_recente, on=["ean"], how="left").filter(
        F.coalesce(F.col("eh_medicamento_consolidado"), F.col("eh_medicamento"))
        == F.lit(eh_medicamento)
    )
