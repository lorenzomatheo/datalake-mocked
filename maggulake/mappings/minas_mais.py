import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from maggulake.utils.strings import string_to_column_name

s3_file_minas_mais = "s3://maggu-datalake-prod/1-raw-layer/maggu/produtos_minas_mais/dados-produtos-minas-mais.xlsx"


class MinasMaisParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_raw_df(self, url=s3_file_minas_mais):
        df = pd.read_excel(url, header=0, dtype={"EAN PREFERENCIAL": str})

        df.columns = [string_to_column_name(col) for col in df.columns]

        return self.spark.createDataFrame(df)

    def to_produtos_raw(self, raw_df):
        if "eans_cadastrados" in raw_df.columns:
            eans_cadastrados_expr = F.when(
                F.col("eans_cadastrados").isNull()
                | (F.trim(F.col("eans_cadastrados")) == ""),
                F.array().cast("array<string>"),
            ).otherwise(
                F.array_remove(
                    F.transform(
                        F.split(F.trim(F.col("eans_cadastrados")), ","),
                        lambda x: F.trim(x),
                    ),
                    "",
                )
            )
        else:
            eans_cadastrados_expr = F.array().cast("array<string>")

        return (
            raw_df.filter(
                F.col("ean_preferencial").isNotNull()
                & (F.col("ean_preferencial") != "")
                & F.col("laboratorio").isNotNull()
                & (F.col("laboratorio") != "")
            )
            .select(
                F.col("ean_preferencial").alias("ean"),
                F.col("laboratorio").alias("fabricante"),
                F.lit("MINAS_MAIS").alias("fonte"),
                eans_cadastrados_expr.alias("eans_alternativos"),
            )
            .dropDuplicates(["ean"])
            .withColumn("gerado_em", F.current_timestamp())
        )
