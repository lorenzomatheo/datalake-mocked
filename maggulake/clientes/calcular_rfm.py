"""Análise de Recência, Frequência, Valor Monetário (RFM). Em outras palavras,
uma coleção que realiza análise RFM em um determinado conjunto de dados.
Ref: ALGORITMOS DE CLUSTERIZAÇÃO E O MODELO RFV (RECÊNCIA, FREQUÊNCIA E VALOR)
APLICADOS EM DADOS DE E-COMMERCE. Trabalho de Conclusão de Curso apresentado
ao curso de Graduação em Engenharia de Produção, Setor de Tecnologia,
Universidade Federal do Paraná, como requisito parcial à obtenção do título
de Bacharel em Engenharia de Produção. Orientadora: Prof. Mariana Kleina

# R: quanto menor o valor, mais recente é o cliente
# F: quanto menor o valor, menor é a quantidade (freq) total de compras
# V: quanto menor o valor, menor o ticket médio do cliente
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from maggulake.enums import RFMCategories
from maggulake.utils.numbers import assign_quintile


def calculate_rfm(
    data: DataFrame,
    recency_col: str = "data_ultimo_pedido",
    frequency_col: str = "qtde_pedidos",
    monetary_col: str = "ticket_medio",
) -> DataFrame:
    """Runs a RFM analysis on the given dataset. The user can specify the
    columns to use for the analysis. It is mandatory to have a column for
    recency, frequency and monetary value.
    """

    def assign_quintile_wrapper(row):
        # Só existe isso pq o pyspark nao aceita funções com mais de 1 argumento
        return assign_quintile(
            value=row["value"], quintiles=row["quintiles"], ascending=row["ascending"]
        )

    assign_quintile_udf = F.udf(assign_quintile_wrapper, T.IntegerType())

    data = (
        data.withColumn("R", F.datediff(F.current_date(), F.col(recency_col)))
        .withColumn("F", F.col(frequency_col))
        .withColumn("V", F.col(monetary_col))
    )

    metrics = ["R", "F", "V"]
    quintiles = {
        metric: data.approxQuantile(metric, [0.2, 0.4, 0.6, 0.8], 0.01)
        for metric in metrics
    }

    for metric in metrics:
        data = data.withColumn(
            metric,
            assign_quintile_udf(
                F.struct(
                    F.col(metric).alias("value"),
                    F.array(*[F.lit(q) for q in quintiles[metric]]).alias("quintiles"),
                    F.lit(metric == "R").alias("ascending"),
                )
            ),
        )

    # cria uma coluna com a média dos valores de F e de V
    data = data.withColumn("FV", (F.col("F") + F.col("V")) / 2)
    rfv_conditions = [
        (
            (F.col("R").between(4, 5)) & (F.col("FV").between(4, 5)),
            RFMCategories.CHAMPIONS.value,
        ),
        (
            (F.col("R").between(2, 5)) & (F.col("FV").between(3, 5)),
            RFMCategories.LOYAL_CUSTOMERS.value,
        ),
        (
            (F.col("R").between(3, 5)) & (F.col("FV").between(1, 3)),
            RFMCategories.POTENTIAL_LOYALIST.value,
        ),
        (
            (F.col("R").between(4, 5)) & (F.col("FV").between(0, 1)),
            RFMCategories.NEW_CUSTOMERS.value,
        ),
        (
            (F.col("R").between(3, 4)) & (F.col("FV").between(0, 1)),
            RFMCategories.PROMISING.value,
        ),
        (
            (F.col("R").between(2, 3)) & (F.col("FV").between(2, 3)),
            RFMCategories.NEEDING_ATTENTION.value,
        ),
        (
            (F.col("R").between(2, 3)) & (F.col("FV").between(0, 2)),
            RFMCategories.ABOUT_TO_SLEEP.value,
        ),
        (
            (F.col("R").between(0, 2)) & (F.col("F").between(2, 5)),
            RFMCategories.AT_RISK.value,
        ),
        (
            (F.col("R").between(0, 1)) & (F.col("FV").between(4, 5)),
            RFMCategories.CANT_LOSE.value,
        ),
        (
            (F.col("R").between(1, 2)) & (F.col("FV").between(1, 2)),
            RFMCategories.HIBERNATING.value,
        ),
        (
            (F.col("R").between(0, 2)) & (F.col("FV").between(0, 2)),
            RFMCategories.LOST.value,
        ),
    ]

    rfv_column = F.when(rfv_conditions[0][0], rfv_conditions[0][1])
    for condition, value in rfv_conditions[1:]:
        rfv_column = rfv_column.when(condition, value)

    data = data.withColumn("RFV", rfv_column.otherwise(RFMCategories.GENERAL.value))
    return data.drop("FV")
