import pyspark.sql.types as T

schema_enums = T.StructType(
    [
        T.StructField("nome_enum", T.StringType(), False),
        T.StructField("valor", T.StringType(), False),
        T.StructField("valor_normalizado", T.StringType(), True),
        T.StructField("ordem_logica", T.IntegerType(), True),
    ]
)
