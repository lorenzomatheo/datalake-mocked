import pyspark.sql.types as T

schema_product_association_rules = T.StructType(
    [
        T.StructField("produto_a_id", T.StringType(), False),
        T.StructField("produto_b_id", T.StringType(), False),
        T.StructField("produto_a_ean", T.StringType(), False),
        T.StructField("produto_b_ean", T.StringType(), False),
        T.StructField("produto_a_nome", T.StringType(), True),
        T.StructField("produto_b_nome", T.StringType(), True),
        T.StructField("support", T.DoubleType(), False),
        T.StructField("confidence", T.DoubleType(), False),
        T.StructField("lift", T.DoubleType(), False),
        T.StructField("conviction", T.DoubleType(), True),
        T.StructField("total_transacoes", T.LongType(), False),
        T.StructField("transacoes_produto_a", T.LongType(), False),
        T.StructField("transacoes_produto_b", T.LongType(), False),
        T.StructField("transacoes_ambos_produtos", T.LongType(), False),
        T.StructField("algoritmo_usado", T.StringType(), False),
        T.StructField("periodo_analise_dias", T.IntegerType(), False),
        T.StructField("data_calculo", T.TimestampType(), False),
    ]
)

schema_category_association_rules = T.StructType(
    [
        T.StructField("categoria_a", T.StringType(), False),
        T.StructField("categoria_b", T.StringType(), False),
        T.StructField("support", T.DoubleType(), False),
        T.StructField("confidence", T.DoubleType(), False),
        T.StructField("lift", T.DoubleType(), False),
        T.StructField("conviction", T.DoubleType(), True),
        T.StructField("total_transacoes", T.LongType(), False),
        T.StructField("transacoes_categoria_a", T.LongType(), False),
        T.StructField("transacoes_categoria_b", T.LongType(), False),
        T.StructField("transacoes_ambas_categorias", T.LongType(), False),
        T.StructField("algoritmo_usado", T.StringType(), False),
        T.StructField("periodo_analise_dias", T.IntegerType(), False),
        T.StructField("data_calculo", T.TimestampType(), False),
    ]
)
