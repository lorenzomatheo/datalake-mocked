from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

schema_estoque_historico = StructType(
    [
        StructField("criado_em", TimestampType(), True),
        StructField("atualizado_em", TimestampType(), True),
        StructField("id", StringType(), True),
        StructField("ean_conta_loja", StringType(), True),
        StructField("produto_id", StringType(), True),
        StructField("ean", StringType(), True),
        StructField("tenant", StringType(), True),
        StructField("codigo_loja", StringType(), True),
        StructField("custo_compra", DecimalType(), True),
        StructField("preco_venda_desconto", DecimalType(), True),
        StructField("estoque_unid", DoubleType(), True),
        StructField("loja_id", StringType(), True),
        StructField("tipo_ean", StringType(), True),
        StructField("data_extracao", TimestampType(), True),
        StructField("ano_mes", StringType(), True),
    ]
)
