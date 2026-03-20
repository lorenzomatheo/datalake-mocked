import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from maggulake.enums.medicamentos import TiposTarja
from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.utils.strings import string_to_column_name

TABELA_ANVISA = "http://docs.google.com/spreadsheets/d/e/2PACX-1vS300zTw9pJbAlDqkWpdY3hrszpmZ6EdTC_PhvXvsbFm3JvpGSv3pQf8JZJVSfkiw/pub?gid=1469937657&single=true&output=csv&range=A42:BU"


class AnvisaParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_raw_df(self, url=TABELA_ANVISA):
        df = pd.read_csv(url)
        df.columns = [string_to_column_name(col) for col in df.columns]

        return self.spark.createDataFrame(df)

    def to_produtos_raw(self, raw_df):
        produtos_raw_anvisa_pre_eans_alternativos = (
            raw_df.selectExpr(
                "substancia as principio_ativo",
                "laboratorio as fabricante",
                "ean_1 as ean1",
                "ean_2 as ean2",
                "produto as marca",
                "apresentacao as nome_complemento",
                "array(classe_terapeutica) as classes_terapeuticas",
                "tipo_de_produto_status_do_produto as tipo_medicamento",
                "tarja as tarja",
            )
            .replace(
                {
                    "- (*) ": None,
                    "Tarja Vermelha": "Vermelha",
                    "Tarja Vermelha sob restrição": "Vermelha",
                    "Tarja Sem Tarja": None,
                    "Tarja Preta": "Preta",
                },
                subset=["tarja"],
            )
            .replace(
                {
                    "Radiofármaco": TipoMedicamento.RADIOFARMACO.value,
                    "    -     ": None,
                    "BIOLOGICO": TipoMedicamento.BIOLOGICO.value,
                    "Específico": TipoMedicamento.ESPECIFICO.value,
                    "Novo": TipoMedicamento.REFERENCIA.value,
                    "Similar": TipoMedicamento.SIMILAR.value,
                    "Genérico": TipoMedicamento.GENERICO.value,
                    "Fitoterápico": TipoMedicamento.FITOTERAPICO.value,
                },
                subset=["tipo_medicamento"],
            )
            .replace("    -     ", None, subset=["ean1", "ean2"])
            .withColumn("nome", F.concat("marca", F.lit(", "), "nome_complemento"))
            .withColumn(
                "principio_ativo",
                F.when(F.length("principio_ativo") < 1000, F.col("principio_ativo")),
            )
            .withColumn("eh_medicamento", F.lit(True))
            .withColumn("eh_tarjado", F.col("tarja").isNotNull())
            .withColumn(
                "eh_controlado",
                (F.col("tarja") == TiposTarja.VERMELHA_SOB_RESTRICAO.value)
                | (F.col("tarja") == TiposTarja.PRETA.value),
            )
            .withColumn("gerado_em", F.current_timestamp())
            .withColumn("fonte", F.lit("Anvisa"))
            .cache()
        )

        return (
            produtos_raw_anvisa_pre_eans_alternativos.filter("ean1 is not null")
            .selectExpr(
                "*", "ean1 as ean", "array_compact(array(ean2)) as eans_alternativos"
            )
            .unionByName(
                produtos_raw_anvisa_pre_eans_alternativos.filter(
                    "ean2 is not null"
                ).selectExpr(
                    "*",
                    "ean2 as ean",
                    "array_compact(array(ean1)) as eans_alternativos",
                )
            )
            .dropDuplicates(["ean"])
            # TODO: adicionar mais campos na raw...
            # A coluna "eans_alternativos" já foi adicionada na raw, então pode retirar desse drop
            # Quando for adicionar `tarja` na raw, pode retirar também desse drop
            .drop("ean1", "ean2", "eans_alternativos", "tarja")
        )
