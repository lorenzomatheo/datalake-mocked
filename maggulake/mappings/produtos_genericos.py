import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from maggulake.enums.tipo_medicamento import TipoMedicamento


class ProdutosGenericosParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_produtos_raw(self, raw_df: DataFrame) -> DataFrame:
        return (
            raw_df.selectExpr(
                "ean::string AS ean",
                "molecula AS principio_ativo",
                "apresentacao_do_produto AS nome",
                "fabricante",
                "concentracao AS dosagem",
            )
            .withColumn("eh_medicamento", F.lit(True))
            .withColumn("tipo_medicamento", F.lit(TipoMedicamento.GENERICO.value))
            .withColumn("fonte", F.lit("ProdutosGenericos"))
            .dropDuplicates(["ean"])
        )
