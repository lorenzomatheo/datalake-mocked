import pyspark.sql.types as T

schema_view_vendas_eans_campanhas = T.StructType(
    [
        T.StructField("campanha", T.StringType(), True),
        T.StructField("ean_campanha", T.StringType(), True),
        T.StructField("numero_vendas_ean_campanha", T.LongType(), True),
        T.StructField("media_vendas_semanal_ean_campanha", T.DoubleType(), True),
        T.StructField("valor_total_vendas_ean_campanha", T.DoubleType(), True),
        T.StructField("preco_medio_venda_ean_campanha", T.DoubleType(), True),
        T.StructField("numero_lojas_distintas_ean_campanha", T.LongType(), True),
        T.StructField("vendas_por_loja_por_semana_ean_campanha", T.DoubleType(), True),
        T.StructField("nome_produto_campanha", T.StringType(), True),
        T.StructField("concat_ean_nome", T.StringType(), True),
    ]
)

schema_view_vendas_substitutos = T.StructType(
    [
        T.StructField("campanha", T.StringType(), True),
        T.StructField("ean_campanha", T.StringType(), True),
        T.StructField("numero_vendas_ean_campanha", T.LongType(), True),
        T.StructField("media_vendas_semanal_ean_campanha", T.DoubleType(), True),
        T.StructField("valor_total_vendas_ean_campanha", T.DoubleType(), True),
        T.StructField("preco_medio_venda_ean_campanha", T.DoubleType(), True),
        T.StructField("numero_lojas_distintas_ean_campanha", T.LongType(), True),
        T.StructField("vendas_por_loja_por_semana_ean_campanha", T.DoubleType(), True),
        T.StructField("nome_produto_campanha", T.StringType(), True),
        T.StructField("concat_ean_nome", T.StringType(), True),
        T.StructField("ean_substituto", T.StringType(), True),
        T.StructField("numero_vendas_sub", T.DoubleType(), True),
        T.StructField("media_vendas_semanal_sub", T.DoubleType(), True),
        T.StructField("valor_total_vendas_sub", T.DoubleType(), True),
        T.StructField("preco_medio_venda_sub", T.DoubleType(), True),
        T.StructField("numero_lojas_distintas_sub", T.DoubleType(), True),
        T.StructField("vendas_por_loja_por_semana_sub", T.DoubleType(), True),
        T.StructField("numero_vendas_20p", T.DoubleType(), True),
        T.StructField("valor_vendas_20p", T.DoubleType(), True),
        T.StructField("faturamento_total_apos_20p", T.DoubleType(), True),
        T.StructField("faturamento_total_antes_20p", T.DoubleType(), True),
        T.StructField("nome_produto_substituto", T.StringType(), True),
        T.StructField("concat_ean_substituto_nome", T.StringType(), True),
    ]
)
