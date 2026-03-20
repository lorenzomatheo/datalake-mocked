import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from maggulake.utils.strings import string_to_column_name

s3_file_iqvia = "1-raw-layer/maggu/produtos_iqvia/TbDinamica_PMB_Full - Abr24.xlsm"


class IqviaParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_raw_df(self, url):
        df = pd.read_excel(
            url,
            sheet_name='tbDinamica',
            skiprows=1,
            header=0,
        )

        df.columns = [string_to_column_name(col) for col in df.columns]

        return self.spark.createDataFrame(df)

    def to_produtos_raw(self, raw_df: DataFrame) -> DataFrame:
        # TODO: Da pra pegar também eh_otc, eh_controlado, tipo_medicamento (talvez) e forma_farmaceutica, se conseguirmos padronizar

        return (
            raw_df.withColumn(
                "categorias",
                agrega_categoria("cc_1_desc", "cc_2_desc", "cc_3_desc", "cc_4_desc"),
            )
            .selectExpr(
                # "lpad(cd_ean::int, 13, '0') as ean",
                "cd_ean::long::string as ean",
                "molecula_br as principio_ativo",
                "produto as marca",
                "apresentacao as nome",
                "laboratorio as fabricante",
                "fg_medicamento as eh_medicamento",
                "categorias",
                "`otc/rx` <=> 'E' as eh_tarjado",
            )
            .replace("N/I", None, "principio_ativo")
            .withColumn("fonte", F.lit("IQVIA"))
            .withColumn("gerado_em", F.current_timestamp())
        )


@F.udf("array<string>")
def agrega_categoria(cc1, cc2, cc3, cc4):
    if cc1 == 'NOT OTC':
        return None

    sem_repetidos = list(dict.fromkeys([c for c in [cc1, cc2, cc3, cc4] if c]))

    return [" -> ".join(sem_repetidos)] if sem_repetidos else None
