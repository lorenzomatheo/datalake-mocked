import pyspark.sql.types as T

schema_websearch_marca_fabricante = T.StructType(
    [
        T.StructField("ean", T.StringType(), True),
        T.StructField("marca_websearch", T.StringType(), True),
        T.StructField("fabricante_websearch", T.StringType(), True),
        T.StructField("websearch_em", T.TimestampType(), True),
    ]
)
