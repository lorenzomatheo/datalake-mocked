import re
from io import StringIO

import pandas as pd
import pyspark.sql.functions as F
import requests
from pyspark.sql import DataFrame, SparkSession

from maggulake.enums.faixa_etaria import FaixaEtariaSimplificado
from maggulake.enums.medicamentos import TiposTarja
from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.utils.strings import string_to_column_name

medicamentos_preco = "https://dados.anvisa.gov.br/dados/TA_PRECO_MEDICAMENTO.csv"
medicamentos_restricao = (
    "https://dados.anvisa.gov.br/dados/TA_RESTRICAO_MEDICAMENTO.csv"
)
PRECOS_CSV_HEADER_ROWS = 41


class AnvisaMedicamentosParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_raw_precos_df(self, url=medicamentos_preco):
        return self.to_df(
            self.read_csv(url, encoding="utf8"),
            skiprows=PRECOS_CSV_HEADER_ROWS,
        )

    def to_raw_restricoes_df(self, url=medicamentos_restricao):
        return self.to_df(self.read_csv(url))

    def to_produtos_raw(
        self, raw_precos_df: DataFrame, raw_restricoes_df: DataFrame
    ) -> DataFrame:
        medicamentos_preco_df = self.parse_precos(raw_precos_df)
        medicamentos_restricao_df = self.parse_restricoes(raw_restricoes_df)

        return (
            medicamentos_preco_df.alias("p")
            .join(medicamentos_restricao_df.alias("r"), "marca")
            .selectExpr(
                "EXPLODE(eans_alternativos) AS ean",
                "*",
            )
            .withColumn(
                "eans_alternativos", F.array_except("eans_alternativos", F.array("ean"))
            )
            .dropDuplicates(["ean"])
        )

    def parse_precos(self, raw_precos_df: DataFrame) -> DataFrame:
        return (
            raw_precos_df.selectExpr(
                "ARRAY(ean_1, ean_2, ean_3) AS eans_alternativos",
                "CONCAT(produto, ' ', apresentacao) AS nome",
                "UPPER(produto) AS marca",
                "substancia AS principio_ativo",
                "laboratorio AS fabricante",
                "apresentacao AS nome_complemento",
                "tipo_de_produto_status_do_produto AS tipo_medicamento",
                "tarja AS tarja",
                "registro AS numero_registro",
            )
            .withColumn(
                "eans_alternativos",
                F.filter("eans_alternativos", lambda e: F.trim(e) != "-"),
            )
            .withColumn(
                "tarja",
                F.when(F.col("tarja") == "Tarja Vermelha", TiposTarja.VERMELHA)
                .when(
                    F.col("tarja") == "Tarja Vermelha sob restrição",
                    TiposTarja.VERMELHA_SOB_RESTRICAO,
                )
                .when(F.col("tarja") == "Tarja Preta", TiposTarja.PRETA)
                .when(F.col("tarja") == "Tarja Sem Tarja", TiposTarja.SEM_TARJA),
            )
            .withColumn(
                "eh_medicamento",
                F.lit(True),
            )
            .withColumn(
                "eh_tarjado",
                F.startswith(F.col("tarja"), F.lit("Vermelha"))
                | (F.col("tarja") == TiposTarja.PRETA),
            )
            .withColumn(
                "eh_controlado",
                (F.col("tarja") == TiposTarja.VERMELHA_SOB_RESTRICAO)
                | (F.col("tarja") == TiposTarja.PRETA),
            )
            .withColumn("eh_otc", F.col("tarja") == TiposTarja.SEM_TARJA)
            .withColumn(
                "principio_ativo",
                F.when(
                    F.length(F.col("principio_ativo")) < 1000, F.col("principio_ativo")
                ),
            )
            .replace(
                {
                    "Radiofármaco": TipoMedicamento.RADIOFARMACO.value,
                    "Biológico": TipoMedicamento.BIOLOGICO.value,
                    "Específico": TipoMedicamento.ESPECIFICO.value,
                    "Novo": TipoMedicamento.REFERENCIA.value,
                    "Similar": TipoMedicamento.SIMILAR.value,
                    "Dinamizado": TipoMedicamento.DINAMIZADO.value,
                    "Fitoterápico": TipoMedicamento.FITOTERAPICO.value,
                    "Genérico": TipoMedicamento.GENERICO.value,
                    "    -     ": None,
                },
                subset=["tipo_medicamento"],
            )
            .withColumn("gerado_em", F.current_timestamp())
            .withColumn("fonte", F.lit("AnvisaMedicamentos"))
        )

    def parse_restricoes(self, raw_restricoes_df: DataFrame) -> DataFrame:
        # Glossario: https://dados.anvisa.gov.br/dados/Documentacao_e_Dicionario_Restricao_Medicamento_V1.pdf
        # Coluna `DS_CONCENTRACAO`: Descrição da concentração, podendo ser em pó, capsula e gota
        # NOTE: Infelizmente falta a unidade de medida. Nota zero pra ANVISA. Optei por não utilizar essa informação.
        return (
            raw_restricoes_df.selectExpr(
                "UPPER(no_produto) AS marca",
                "ARRAY(ds_categoria_produto) AS categorias",
                "ds_forma_fisica AS forma_farmaceutica",
                "ds_restricao_uso AS idade_recomendada",
            )
            .withColumn(
                "idade_recomendada",
                F.when(
                    F.startswith("idade_recomendada", F.lit("Adulto e Pediátrico")),
                    FaixaEtariaSimplificado.TODOS,
                )
                .when(
                    F.startswith("idade_recomendada", F.lit("Adulto")),
                    FaixaEtariaSimplificado.ADULTO,
                )
                .when(
                    F.startswith("idade_recomendada", F.lit("Pediátrico")),
                    FaixaEtariaSimplificado.CRIANCA,
                ),
            )
            .dropDuplicates(["marca"])
        )

    def read_csv(self, url: str, encoding: str = "latin1"):
        request = requests.get(url, verify=False)
        text = request.content.decode(encoding)

        return re.sub(r"&\#\d{4};", "", text)

    def to_df(self, csv: str, skiprows: int = 0, usecols: list = None):
        pandas_df = pd.read_csv(
            StringIO(csv), sep=";", skiprows=skiprows, usecols=usecols
        )

        pandas_df.columns = [string_to_column_name(col) for col in pandas_df.columns]

        return self.spark.createDataFrame(pandas_df)
