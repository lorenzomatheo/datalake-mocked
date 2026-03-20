import pyspark.sql.types as T

schema_sftp_tradefy = T.StructType(
    [
        T.StructField("I", T.StringType(), True),
        T.StructField("CNPJ_Filial_Distribuidor", T.StringType(), True),
        T.StructField("CNPJ_PDV", T.StringType(), True),
        T.StructField("Data_Nota_Fiscal", T.StringType(), True),
        T.StructField("Numero_Nota_Fiscal", T.StringType(), True),
        T.StructField("Tipo_Documento", T.StringType(), True),
        T.StructField("Tipo_Envio", T.StringType(), True),
        T.StructField("Canal_Venda", T.StringType(), True),
        T.StructField("Cod_Prod", T.StringType(), True),
        T.StructField("DUN", T.StringType(), True),
        T.StructField("EAN", T.StringType(), True),
        T.StructField("Venda_Bruta", T.DoubleType(), True),
        T.StructField("Venda_Liquida", T.DoubleType(), True),
        T.StructField("Venda_Unidades", T.DoubleType(), True),
        T.StructField("Preco_Sku_NF", T.DoubleType(), True),
    ]
)
