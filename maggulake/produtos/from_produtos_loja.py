from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def to_produtos(produtos_loja: DataFrame, schema: T.StructType) -> DataFrame:
    return agrupa_produtos_de_diferentes_lojas(
        extrai_substituicoes_produto_v2(produtos_loja, schema)
    )


def extrai_substituicoes_produto_v2(
    produtos_loja: DataFrame, schema: T.StructType
) -> DataFrame:
    return (
        produtos_loja.filter(
            "substituicoes_produto_v2 IS NOT NULL AND substituicoes_produto_v2 != '{}'"
        )
        .withColumn(
            "substituicoes_produto_v2",
            F.from_json("substituicoes_produto_v2", schema).dropFields("id", "ean"),
        )
        .selectExpr("produto_id AS id", "ean", "substituicoes_produto_v2.*")
        .drop("fabricante")
        # Esse campo nao esta na api (deveria), mas antigamente pegavamos isso
        .withColumn("fonte", F.lit("Clientes"))
        .withColumnRenamed("marca", "fabricante")
    )


def agrupa_produtos_de_diferentes_lojas(produtos: DataFrame) -> DataFrame:
    colunas_texto = [
        "nome",
        "descricao",
        "fabricante",
        "principio_ativo",
    ]

    def maior_texto(coluna: str):
        remove_vazios = F.when(F.col(coluna) != "", coluna)
        return F.max_by(coluna, F.length(remove_vazios)).alias(coluna)

    return produtos.groupBy("id", "ean").agg(
        *[maior_texto(coluna) for coluna in colunas_texto],
        *[
            F.first(c, ignorenulls=True).alias(c)
            for c in produtos.drop("id", "ean", *colunas_texto).columns
        ],
    )
