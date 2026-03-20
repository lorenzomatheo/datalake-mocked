from collections.abc import Callable

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pymilvus import MilvusClient
from pyspark.sql import DataFrame

from maggulake.utils.iters import batched


class Milvus:
    def __init__(self, uri, token):
        self.client = MilvusClient(
            uri=uri,
            token=token,
        )

    def formata_produtos(self, produtos_com_embeddings):
        colunas_filtros_texto = [
            "principio_ativo",
            "via_administracao",
            "tipo_medicamento",
        ]

        def padroniza_texto(coluna):
            return F.lower(F.trim(coluna))

        for coluna in colunas_filtros_texto:
            produtos_com_embeddings = produtos_com_embeddings.withColumn(
                coluna,
                padroniza_texto(coluna),
            )

        produtos_com_embeddings = produtos_com_embeddings.withColumn(
            "categorias",
            F.transform("categorias", padroniza_texto),
        )

        colunas_texto_menores = [
            coluna.name
            for coluna in produtos_com_embeddings.schema
            if coluna.dataType == T.StringType()
            and coluna.name not in ["lojas_com_produto", "informacoes_para_embeddings"]
        ]

        for coluna in colunas_texto_menores:
            produtos_com_embeddings = produtos_com_embeddings.withColumn(
                coluna,
                F.substring(coluna, 0, 10000),
            )

        return (
            produtos_com_embeddings.withColumn(
                'lojas_com_produto', F.coalesce(F.col('lojas_com_produto'), F.lit(""))
            )
            .withColumn('gerado_em', F.date_format('gerado_em', "MM/dd/yyyy hh:mm"))
            .withColumn(
                'atualizado_em', F.date_format('atualizado_em', "MM/dd/yyyy hh:mm")
            )
            .withColumn("ean", F.substring("ean", 0, 20))
            .withColumn("power_phrase", F.substring("power_phrase", 0, 500))
            .withColumn(
                "principio_ativo",
                F.substring(F.col('principio_ativo'), 0, 1000),
            )
            .withColumn(
                'eh_nao_tarjado',
                F.expr('eh_tarjado is not null and eh_tarjado = false'),
            )
            .withColumn(
                'eh_nao_controlado',
                F.expr('eh_controlado is not null and eh_controlado = false'),
            )
            .drop("bula")
        )

    def upsert(
        self,
        registros: DataFrame,
        collection_name: str,
        batch_size: int = 100,
        process_row_function: Callable[[dict], None] | None = None,
    ):
        total = registros.count()
        n = 0

        for i, batch in enumerate(batched(registros.toLocalIterator(), batch_size)):
            print(f"Fazendo batch {i}")
            data = [row.asDict(recursive=True) for row in batch]

            if process_row_function:
                for row in data:
                    process_row_function(row)

            self.client.upsert(collection_name=collection_name, data=data)
            n += len(batch)
            print(f"Sucesso! {n} registros atualizados ({n * 100 / total:.2f}%)")

        print(f"Processo concluído. Total de {n} registros atualizados.")
