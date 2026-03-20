from abc import ABC, abstractmethod

import pyspark.sql.functions as F
import requests.exceptions
from pyspark.sql import DataFrame

from maggulake.environment.tables import Table
from maggulake.produtos.valida_ean import valida_ean
from maggulake.utils.iters import batched
from maggulake.utils.time import agora_em_sao_paulo


class BuscadorBase(ABC):
    LIMITE_PESQUISAS = 1000

    tabela: Table
    schema_tabela: str
    fonte: str

    @abstractmethod
    def __init__(self, env):
        self.spark = env.spark
        env.create_table_if_not_exists(self.tabela, self.schema_tabela)

    def pesquisa_eans(self, eans_df: DataFrame) -> DataFrame:
        cache = self.spark.read.table(self.tabela.value)

        eans_novos_df = eans_df.join(cache, "ean", "leftanti")
        eans_novos_validos = [
            e.ean for e in eans_novos_df.toLocalIterator() if valida_ean(e.ean)
        ][: self.LIMITE_PESQUISAS]

        self._salva_novos_produtos(eans_novos_validos)

        if eans_novos_validos:
            cache = self.spark.read.table(self.tabela.value)

        return (
            cache.join(eans_df, "ean", "semi")
            .filter("encontrou = true")
            .withColumn("gerado_em", F.col("buscado_em"))
            .drop("encontrou", "buscado_em")
            .withColumn("fonte", F.lit(self.fonte))
        )

    def _get_produto(self, ean: str) -> dict:
        buscado_em = agora_em_sao_paulo()
        response = self.client.procura_info_produto_por_ean(ean)

        if response:
            return {**response, "encontrou": True, "buscado_em": buscado_em}

        return {"ean": ean, "encontrou": False, "buscado_em": buscado_em}

    def _salva_novos_produtos(self, eans: list[str]):
        for b in batched(eans, 100):
            responses = []
            limite_chamadas_excedido = False
            for e in b:
                try:
                    responses.append(self._get_produto(e))
                except requests.exceptions.HTTPError:
                    limite_chamadas_excedido = True
                    break

            if responses:
                df = self.spark.createDataFrame(responses, self.schema_tabela)
                df.write.mode("append").saveAsTable(self.tabela.value)

            if limite_chamadas_excedido:
                return
