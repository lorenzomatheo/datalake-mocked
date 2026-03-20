import json
import re

import pyspark.sql.functions as F
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

from maggulake.enums.medicamentos import TiposTarja
from maggulake.io.repositorio_s3 import RepositorioS3


class RdParser:
    def __init__(self, spark: SparkSession, stage: str):
        self.spark = spark
        self.stage = stage

    def to_raw_df(self):
        path = self.latest_path()
        rdd = self.spark.sparkContext.wholeTextFiles(path)

        return rdd.flatMap(self.parse_page).toDF(sampleRatio=1)

    @staticmethod
    def parse_page(page):
        filename, page = page
        data = json.loads(page)
        entries = data['data']['products']['items']

        return [RdParser.parse_entry(filename, e) for e in entries]

    @staticmethod
    def parse_entry(filename: str, entry: dict):
        def get_custom_attribute(custom_attribute_dict: dict):
            if custom_attribute_dict["value_string"]:
                return custom_attribute_dict["value_string"][0]
            elif custom_attribute_dict["value"]:
                return custom_attribute_dict["value"][0]["label"]

            return None

        custom_attributes = {
            c["attribute_code"]: custom_attribute
            for c in entry["custom_attributes"]
            if (custom_attribute := get_custom_attribute(c)) is not None
            and custom_attribute != ""
        }

        return {
            "arquivo_s3": filename,
            "sku": entry["sku"],
            "name": entry["name"],
            "price": entry["price"],
            "breadcrumb": [i.get("name") for i in (entry.get("breadcrumb") or [])],
            "media_gallery_entries": [
                i["file"] for i in entry["media_gallery_entries"]
            ],
            "pbm_id": entry["pbm"]["id"] if entry["pbm"] else None,
            **custom_attributes,
        }

    def to_produtos_raw(self, raw_df):
        @F.udf("string")
        def extract_text_with_beautiful_soup(text):
            new_text = BeautifulSoup(text, features="html.parser").get_text(
                separator="\n"
            )
            # A descrição pode vir com vários códigos unicodes do tipo `_x005F_x005F_x005F_x005F_x005F_x005F_x005F_x000D_`.
            # Como todos eles são lixos, basta removê-los.
            return re.sub(r'(_x([0-9A-F]{4}))+_', '', new_text)

        raw_df = raw_df.select(
            F.col('name').alias("nome"),
            F.col('media_gallery_entries').getItem(0).alias("imagem_url"),
            "ean",
            F.col('principioativonovo').alias("principio_ativo"),
            F.col("marca").alias("marca"),
            F.col("fabricante").alias("fabricante"),
            extract_text_with_beautiful_soup('description').alias("descricao"),
            F.col('breadcrumb').alias("categorias"),
            F.col('dosagem').alias("dosagem"),
            (F.coalesce(F.col('codtarja'), F.lit(0)) > 0).alias("eh_medicamento"),
            (F.coalesce(F.col('codtarja'), F.lit(0)) > 1).alias("eh_tarjado"),
            F.col('descricaotarja').alias('desc_tarja'),
            "arquivo_s3",
        )

        return (
            raw_df
            # 1. Define eh_otc (True = apenas MEDICAMENTO NAO TARJADO)
            .withColumn(
                "eh_otc",
                F.when(
                    F.col("eh_medicamento")
                    & (F.col("desc_tarja") == "MEDICAMENTO NAO TARJADO"),
                    F.lit(True),
                )
                .when(
                    F.col("eh_medicamento")
                    & F.col("desc_tarja").isin(
                        [
                            "TARJADO EM VERMELHO SEM RETENCAO DA RECEITA",
                            "TARJADO EM VERMELHO COM RETENCAO DA RECEITA",
                            "TARJA PRETA COM RETENCAO DA RECEITA",
                            "TARJA PRETA",
                        ]
                    ),
                    F.lit(False),
                )
                .otherwise(F.lit(None)),
            )
            # 2. Define eh_controlado (medicamentos com retenção de receita)
            .withColumn(
                "eh_controlado",
                F.when(
                    F.col("eh_medicamento")
                    & F.col("desc_tarja").isin(
                        [
                            "TARJADO EM VERMELHO COM RETENCAO DA RECEITA",
                            "TARJA PRETA COM RETENCAO DA RECEITA",
                            "TARJA PRETA",
                        ]
                    ),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            # 3. Define tarja
            .withColumn(
                "tarja",
                F.when(
                    F.col("eh_medicamento"),
                    F.when(
                        F.col("desc_tarja")
                        == "TARJADO EM VERMELHO SEM RETENCAO DA RECEITA",
                        F.lit(TiposTarja.VERMELHA.value),
                    )
                    .when(
                        F.col("desc_tarja")
                        == "TARJADO EM VERMELHO COM RETENCAO DA RECEITA",
                        F.lit(TiposTarja.VERMELHA_SOB_RESTRICAO.value),
                    )
                    .when(
                        F.col("desc_tarja").isin(
                            [
                                "TARJA PRETA COM RETENCAO DA RECEITA",
                                "TARJA PRETA",
                            ]
                        ),
                        F.lit(TiposTarja.PRETA.value),
                    )
                    .when(
                        F.col("desc_tarja") == "MEDICAMENTO NAO TARJADO",
                        F.lit(TiposTarja.SEM_TARJA.value),
                    )
                    .otherwise(F.lit(None)),
                ).otherwise(F.lit(None)),
            )
            .withColumn("gerado_em", F.current_timestamp())
            .drop("desc_tarja")
            .dropDuplicates(["ean"])
            .withColumn('fonte', F.lit('RD'))
        )

    def latest_path(self) -> str:
        rd_path = RepositorioS3.ultima_pasta('1-raw-layer/maggu/rd/', self.stage)
        rd_path = f"s3://maggu-datalake-{self.stage}/{rd_path}paginas/*.json"

        return rd_path
