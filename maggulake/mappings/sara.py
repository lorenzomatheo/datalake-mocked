import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from maggulake.enums.faixa_etaria import FaixaEtaria
from maggulake.enums.medicamentos import TiposTarja
from maggulake.enums.tipo_medicamento import TipoMedicamento


class SaraParser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def to_produtos_raw(self, raw_df: DataFrame) -> DataFrame:
        restrictions = F.lower(F.col("usage_restrictions"))
        stripe = F.lower(F.col("stripe"))

        return (
            raw_df.selectExpr(
                "ean",
                "product_name",
                "product_description AS descricao",
                "anvisa_code AS registro",
                "active_ingredient AS principio_ativo",
                "drug_unit AS dosagem",
                "drug_unit",
                "drug_quantity",
                "stripe",
                "regulatory_class AS tipo_medicamento",
                "therapeutical_class",
                "administration_routes AS via_administracao",
                "conservation AS temperatura_armazenamento",
                "manufacturer AS fabricante",
                "marketing_company AS marca",
                "usage_restrictions",
            )
            .withColumn(
                "nome",
                F.trim(
                    F.concat_ws(
                        " ",
                        F.col("product_name"),
                        F.col("drug_unit"),
                        F.col("drug_quantity"),
                    )
                ),
            )
            .replace(
                {
                    "Similar": TipoMedicamento.SIMILAR.value,
                    "Biológico": TipoMedicamento.BIOLOGICO.value,
                    "Específico": TipoMedicamento.ESPECIFICO.value,
                    "Fitoterápico": TipoMedicamento.FITOTERAPICO.value,
                    "Genérico": TipoMedicamento.GENERICO.value,
                    "Novo": TipoMedicamento.NOVO.value,
                    "BAIXO RISCO": TipoMedicamento.BAIXO_RISCO.value,
                    "Radiofármaco": TipoMedicamento.RADIOFARMACO.value,
                    "Dinamizado": TipoMedicamento.DINAMIZADO.value,
                    "Produto de Terapia Avançada": TipoMedicamento.TERAPIA_AVANCADA.value,
                },
                subset=["tipo_medicamento"],
            )
            .withColumn(
                "idade_recomendada",
                F.when(
                    restrictions.contains("adulto") & restrictions.contains("pedi"),
                    FaixaEtaria.TODOS.value,
                )
                .when(restrictions.contains("60 anos"), FaixaEtaria.IDOSO.value)
                .when(restrictions.contains("adulto"), FaixaEtaria.ADULTO.value)
                .when(restrictions.contains("pedi"), FaixaEtaria.CRIANCA.value)
                .when(
                    restrictions.contains("venda sem"),
                    FaixaEtaria.TODOS.value,
                ),
            )
            .withColumn(
                "classes_terapeuticas",
                F.when(
                    F.col("therapeutical_class").isNotNull(),
                    F.array(F.col("therapeutical_class")),
                ),
            )
            .withColumn("eh_medicamento", F.lit(True))
            .withColumn(
                "tarja",
                F.when(
                    stripe.contains("preta"),
                    TiposTarja.PRETA.value,
                )
                .when(
                    stripe.contains("restrição") | stripe.contains("restricao"),
                    TiposTarja.VERMELHA_SOB_RESTRICAO.value,
                )
                .when(
                    stripe.contains("vermelha"),
                    TiposTarja.VERMELHA.value,
                )
                .when(
                    stripe.contains("sem"),
                    TiposTarja.SEM_TARJA.value,
                ),
            )
            .withColumn(
                "eh_tarjado",
                F.col("tarja").isNotNull()
                & (F.col("tarja") != TiposTarja.SEM_TARJA.value),
            )
            .withColumn("eh_otc", ~F.col("eh_tarjado"))
            .withColumn(
                "eh_controlado",
                (F.col("tarja") == TiposTarja.VERMELHA_SOB_RESTRICAO.value)
                | (F.col("tarja") == TiposTarja.PRETA.value),
            )
            .withColumn("fonte", F.lit("Sara"))
            .withColumn("gerado_em", F.current_timestamp())
            .drop(
                "product_name",
                "drug_unit",
                "drug_quantity",
                "stripe",
                "therapeutical_class",
                "usage_restrictions",
            )
            .dropna(subset=["ean", "nome"])
            .dropDuplicates(["ean"])
        )
