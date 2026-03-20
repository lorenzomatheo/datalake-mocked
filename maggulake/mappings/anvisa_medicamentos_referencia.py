from io import BytesIO

import pandas as pd
import pdfplumber
import requests
from pyspark.sql import DataFrame, SparkSession

from maggulake.enums.tipo_medicamento import TipoMedicamento

URL_A = "https://www.gov.br/anvisa/pt-br/setorregulado/regularizacao/medicamentos/medicamentos-de-referencia/arquivos/lista-a-incluidos-05022025.pdf"
URL_B = "https://www.gov.br/anvisa/pt-br/setorregulado/regularizacao/medicamentos/medicamentos-de-referencia/arquivos/lista-b-incluidos-06012025.pdf"


class AnvisaMedicamentosReferenciaParser:
    def __init__(self, tabela_a: DataFrame, tabela_b: DataFrame):
        self.tabela_a = tabela_a
        self.tabela_b = tabela_b

        self.df = self.parse()

    def parse(self) -> DataFrame:
        junto = self.tabela_a.withColumnRenamed(
            "FÁRMACO", "principio_ativo"
        ).unionByName(
            self.tabela_b.withColumnRenamed("ASSOCIAÇÃO", "principio_ativo"),
            allowMissingColumns=True,
        )

        return junto.selectExpr(
            "principio_ativo",
            "`DETENTOR` AS fabricante",
            "`MEDICAMENTO` AS marca",
            "`REGISTRO` AS numero_registro",
            "`CONCENTRAÇÃO` AS dosagem",
            "`FORMA FARMACÊUTICA` AS forma_farmaceutica",
            f"'{TipoMedicamento.REFERENCIA.value}' AS tipo_medicamento",
        ).filter("marca != 'N/A'")

    @classmethod
    def from_default_urls(cls, spark: SparkSession):
        return cls(
            PDFTableExtractor(spark, URL_A).get_table(),
            PDFTableExtractor(spark, URL_B).get_table(),
        )


class PDFTableExtractor:
    def __init__(self, spark: SparkSession, url: str):
        self.spark = spark
        self.url = url

    def get_table(self):
        response = requests.get(self.url)

        with pdfplumber.open(BytesIO(response.content)) as pdf:
            table_lines = [
                [self.remove_line_breaks(t) for t in line]
                for page in pdf.pages
                for table in page.extract_tables()
                for line in table
            ]

        return self.spark.createDataFrame(
            pd.DataFrame(table_lines[2:], columns=table_lines[1]),
        )

    @staticmethod
    def remove_line_breaks(text: str | None) -> str | None:
        if text is None:
            return None

        return text.replace("\n", " ")
