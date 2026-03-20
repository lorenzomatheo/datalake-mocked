from functools import reduce

from pyspark.sql import DataFrame

from maggulake.mappings.buscador_base import BuscadorBase


class BuscadorProdutos:
    def __init__(self, buscadores: list[BuscadorBase]):
        self.buscadores = buscadores

    def pesquisa_eans(self, eans_df: DataFrame) -> DataFrame:
        resultados = []
        eans_restantes = eans_df

        for buscador in self.buscadores:
            encontrados = buscador.pesquisa_eans(eans_restantes)
            resultados.append(encontrados)
            eans_restantes = eans_restantes.join(
                encontrados.select("ean"), "ean", "leftanti"
            )

        return reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True),
            resultados,
        )
