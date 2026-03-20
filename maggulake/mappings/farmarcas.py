import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from maggulake.enums.tipo_medicamento import TipoMedicamento
from maggulake.utils.strings import string_to_column_name

TABELA_FARMARCAS = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQwIT1bEMyMeoli1ghqRYQTeau6-eYd3OFE81K5MfSn_z36JZ6xrFI7UEXzZ_IR_w/pub?gid=2035439305&single=true&output=csv"


class FarmarcasParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_raw_df(self, url=TABELA_FARMARCAS):
        df = pd.read_csv(url, dtype={"CODIGO BARRAS PRINCIPAL": str})
        df.columns = [string_to_column_name(col) for col in df.columns]

        return self.spark.createDataFrame(df)

    def to_produtos_raw(self, raw_df: DataFrame) -> DataFrame:
        return (
            raw_df.withColumn(
                "descricao_nova",
                F.concat(
                    F.lit("Descrição: "),
                    "ncm_descricao",
                    F.lit("\nTipo produto: "),
                    "tipo_produto",
                    F.lit("\nGrupo: "),
                    "grupo_principal",
                ),
            )
            .selectExpr(
                "codigo_barras_principal AS ean",
                "TRIM(TRAILING '>' FROM descricao) AS marca",
                "apresentacao AS nome_complemento",
                "fabricante",
                "grupo_principal AS tipo_medicamento",
                "substancia_nome AS principio_ativo",
                "concentracao AS dosagem",
                "descricao_nova AS descricao",
                "tipo_produto AS tipo_produto",
                "grupo_principal AS grupo_principal",
                "'Farmarcas' AS fonte",
            )
            .withColumn("nome", F.concat("marca", F.lit(" "), "nome_complemento"))
            .withColumn(
                "eh_medicamento",
                F.col("grupo_principal").isin(
                    [
                        'PROPAGADO',
                        'SIMILAR',
                        'GENERICO',
                        'MANIPULADO',
                    ]
                ),
            )
            .withColumn(
                "eh_otc",
                F.when(~F.col("eh_medicamento"), True)
                .when(F.contains("tipo_produto", F.lit("OTC/MIP")), True)
                .when(F.contains("tipo_produto", F.lit("RX")), False)
                .when(F.contains("tipo_produto", F.lit("CONTROLADO")), False),
            )
            .withColumn(
                "eh_controlado",
                F.when(~F.col("eh_medicamento"), False)
                .when(F.contains("tipo_produto", F.lit("RX")), True)
                .when(F.contains("tipo_produto", F.lit("CONTROLADO")), True)
                .when(F.contains("tipo_produto", F.lit("OTC/MIP")), False),
            )
            .withColumn(
                "tipo_medicamento",
                F.when(
                    F.col("grupo_principal") == "GENERICO",
                    TipoMedicamento.GENERICO.value,
                ).when(
                    F.col("grupo_principal") == "SIMILAR", TipoMedicamento.SIMILAR.value
                ),
            )
            .drop("tipo_produto", "grupo_principal")
        )
