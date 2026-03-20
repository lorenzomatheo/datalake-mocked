"""
Parser para a Lista de Medicamentos Isentos de Prescrição (LMIP) da ANVISA.

A LMIP é publicada pela Instrução Normativa (IN) nº 285, de 7 de março de 2024,
e contém os princípios ativos, formas farmacêuticas, concentrações máximas
e indicações terapêuticas simplificadas dos medicamentos que podem ser
dispensados sem prescrição médica (MIP/OTC).

Fonte: https://anvisalegis.datalegis.net/action/ActionDatalegis.php?acao=abrirTextoAto&tipo=INM&numeroAto=00000285

TODO: Para uma primeira versão, eu simplesmente baixei os dados e converti
para um .csv usando IA. Idealmente deveriamos ter um web scraper que fizesse a
ingestão automática dos dados de tempos em tempos, mas isso daria muito trabalho.

Anexo 1: Fármacos/Medicamentos sintéticos
Anexo 2: Fitoterápicos
"""

import re
from typing import Optional

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from maggulake.utils.strings import remove_accents

# Schema da tabela LMIP na camada raw
LMIP_SCHEMA = T.StructType(
    [
        T.StructField("principio_ativo", T.StringType(), False),
        T.StructField("principio_ativo_original", T.StringType(), False),
        T.StructField("formas_farmaceuticas", T.ArrayType(T.StringType()), True),
        T.StructField("concentracao_maxima", T.StringType(), True),
        T.StructField("indicacao", T.StringType(), True),
        T.StructField("subgrupo_terapeutico", T.StringType(), True),
        T.StructField("tipo_origem", T.StringType(), False),
        T.StructField("gerado_em", T.TimestampType(), False),
    ]
)


class TipoOrigemLmip:
    """Tipo de origem do medicamento na LMIP."""

    SINTETICO = "SINTETICO"
    FITOTERAPICO = "FITOTERAPICO"


def normaliza_principio_ativo(principio_ativo: str) -> str | None:
    """
    Normaliza o princípio ativo para facilitar o match.

    Remove acentos, converte para minúsculas e remove espaços extras.
    """
    if principio_ativo is None:
        return None
    texto = remove_accents(principio_ativo)
    texto = texto.lower().strip()
    # Remove espaços múltiplos
    texto = re.sub(r"\s+", " ", texto)
    return texto


normaliza_principio_ativo_udf = F.udf(normaliza_principio_ativo, T.StringType())
""" UDF para normalização de princípio ativo"""


def split_formas_farmaceuticas(forma_farmaceutica: str) -> list[str]:
    """
    Divide formas farmacêuticas compostas em uma lista.

    A LMIP pode conter múltiplas formas em uma única célula, separadas por:
    - vírgula: "Creme dermatológico, pomada dermatológica"
    - barra: "Gel / Creme" ou "Comprimido / Cápsula"

    Retorna lista de formas farmacêuticas normalizadas (lowercase, sem acentos).
    """
    if forma_farmaceutica is None:
        return []

    # Normaliza separadores
    texto = forma_farmaceutica.replace(" / ", ", ").replace("/", ", ")

    # Divide por vírgula
    partes = [p.strip() for p in texto.split(",") if p.strip()]

    # Normaliza cada parte
    formas = []
    for parte in partes:
        normalizado = remove_accents(parte).lower().strip()
        # Remove parênteses e conteúdo dentro (ex: "(gotas)" -> "")
        normalizado = re.sub(r"\s*\([^)]*\)\s*", " ", normalizado).strip()
        normalizado = re.sub(r"\s+", " ", normalizado)
        if normalizado:
            formas.append(normalizado)

    return formas


# UDF para split de formas farmacêuticas
split_formas_farmaceuticas_udf = F.udf(
    split_formas_farmaceuticas, T.ArrayType(T.StringType())
)


class AnvisaLmipParser:
    """
    Parser para os anexos da LMIP (Lista de Medicamentos Isentos de Prescrição).

    Anexo 1: Fármacos/Medicamentos sintéticos
    - Colunas: Item, Fármaco, Subgrupo Terapêutico, Forma Farmacêutica,
               Concentração Máxima, Indicação Terapêutica Simplificada

    Anexo 2: Fitoterápicos
    - Colunas: Item, Espécie Vegetal, Parte Utilizada, Indicação Terapêutica Simplificada
    - Mapeamento (conforme orientação do Mario):
        - Fármaco = Espécie Vegetal
        - Subgrupo terapêutico = Classe terapêutica (vazio para fitoterápicos)
        - Forma farmacêutica = Parte Utilizada
        - Concentração máxima = VAZIO
        - Indicação = Indicação Terapêutica Simplificada
    """

    def __init__(
        self,
        medicamentos_df: Optional[DataFrame] = None,
        fitoterapicos_df: Optional[DataFrame] = None,
    ):
        """
        Inicializa o parser com os DataFrames brutos dos anexos.

        Args:
            medicamentos_df: DataFrame bruto do Anexo 1 (fármacos sintéticos)
            fitoterapicos_df: DataFrame bruto do Anexo 2 (fitoterápicos)
        """
        self.raw_medicamentos = medicamentos_df
        self.raw_fitoterapicos = fitoterapicos_df

        # DataFrame unificado após parsing
        self.lmip_df: Optional[DataFrame] = None

        if medicamentos_df is not None or fitoterapicos_df is not None:
            self.lmip_df = self.parse(medicamentos_df, fitoterapicos_df)

    def parse(
        self,
        medicamentos_df: Optional[DataFrame],
        fitoterapicos_df: Optional[DataFrame],
    ) -> DataFrame:
        """
        Processa e unifica os anexos da LMIP.

        Args:
            medicamentos_df: DataFrame bruto do Anexo 1
            fitoterapicos_df: DataFrame bruto do Anexo 2

        Returns:
            DataFrame unificado com schema padronizado (LMIP_SCHEMA)
        """
        dfs = []

        if medicamentos_df is not None:
            parsed_medicamentos = self.parse_medicamentos(medicamentos_df)
            dfs.append(parsed_medicamentos)

        if fitoterapicos_df is not None:
            parsed_fitoterapicos = self.parse_fitoterapicos(fitoterapicos_df)
            dfs.append(parsed_fitoterapicos)

        if not dfs:
            raise ValueError(
                "Pelo menos um dos DataFrames (medicamentos ou fitoterápicos) "
                "deve ser fornecido."
            )

        # Unifica os DataFrames
        if len(dfs) == 1:
            return dfs[0]

        return dfs[0].unionByName(dfs[1])

    def parse_medicamentos(self, raw_df: DataFrame) -> DataFrame:
        """
        Processa o Anexo 1 (fármacos sintéticos).

        Colunas esperadas:
        - Item: número sequencial
        - Fármaco: princípio ativo
        - Subgrupo Terapêutico: classificação terapêutica
        - Forma Farmacêutica: forma de apresentação (pode conter múltiplas separadas por vírgula)
        - Concentração Máxima: concentração máxima permitida para MIP
        - Indicação Terapêutica Simplificada: indicação simplificada
        """
        return (
            raw_df.withColumn("principio_ativo_original", F.col("Fármaco"))
            .withColumn(
                "principio_ativo",
                normaliza_principio_ativo_udf(F.col("Fármaco")),
            )
            .withColumn(
                "formas_farmaceuticas",
                split_formas_farmaceuticas_udf(F.col("Forma Farmacêutica")),
            )
            .withColumn(
                "concentracao_maxima",
                F.trim(F.col("Concentração Máxima")),
            )
            .withColumn(
                "indicacao",
                F.trim(F.col("Indicação Terapêutica Simplificada")),
            )
            .withColumn(
                "subgrupo_terapeutico",
                F.trim(F.col("Subgrupo Terapêutico")),
            )
            .withColumn(
                "tipo_origem",
                F.lit(TipoOrigemLmip.SINTETICO),
            )
            .withColumn(
                "gerado_em",
                F.current_timestamp(),
            )
            .select(
                "principio_ativo",
                "principio_ativo_original",
                "formas_farmaceuticas",
                "concentracao_maxima",
                "indicacao",
                "subgrupo_terapeutico",
                "tipo_origem",
                "gerado_em",
            )
        )

    def parse_fitoterapicos(self, raw_df: DataFrame) -> DataFrame:
        """
        Processa o Anexo 2 (fitoterápicos).

        Colunas esperadas:
        - Item: número sequencial
        - Espécie Vegetal: nome da espécie (mapeado para principio_ativo)
        - Parte Utilizada: parte da planta (mapeado para forma_farmaceutica)
        - Indicação Terapêutica Simplificada: indicação simplificada

        Mapeamento conforme orientação do Mario:
        - Fármaco = Espécie Vegetal
        - Forma farmacêutica = Parte Utilizada
        - Concentração máxima = VAZIO
        - Subgrupo terapêutico = VAZIO (não disponível para fitoterápicos)
        """
        return (
            raw_df.withColumn("principio_ativo_original", F.col("Espécie Vegetal"))
            .withColumn(
                "principio_ativo",
                normaliza_principio_ativo_udf(F.col("Espécie Vegetal")),
            )
            .withColumn(
                "formas_farmaceuticas",
                split_formas_farmaceuticas_udf(F.col("Parte Utilizada")),
            )
            .withColumn(
                "concentracao_maxima",
                F.lit(None).cast(T.StringType()),  # Vazio para fitoterápicos
            )
            .withColumn(
                "indicacao",
                F.trim(F.col("Indicação Terapêutica Simplificada")),
            )
            .withColumn(
                "subgrupo_terapeutico",
                F.lit(None).cast(T.StringType()),  # Não disponível para fitoterápicos
            )
            .withColumn(
                "tipo_origem",
                F.lit(TipoOrigemLmip.FITOTERAPICO),
            )
            .withColumn(
                "gerado_em",
                F.current_timestamp(),
            )
            .select(
                "principio_ativo",
                "principio_ativo_original",
                "formas_farmaceuticas",
                "concentracao_maxima",
                "indicacao",
                "subgrupo_terapeutico",
                "tipo_origem",
                "gerado_em",
            )
        )

    @classmethod
    def from_csv(
        cls,
        spark: SparkSession,
        medicamentos_path: str,
        fitoterapicos_path: str,
    ) -> "AnvisaLmipParser":
        """
        Cria o parser a partir de arquivos CSV locais.

        Args:
            spark: SparkSession
            medicamentos_path: Caminho para o CSV do Anexo 1 (fármacos)
            fitoterapicos_path: Caminho para o CSV do Anexo 2 (fitoterápicos)

        Returns:
            Instância do parser com os DataFrames carregados e processados
        """
        medicamentos_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .csv(medicamentos_path)
        )

        fitoterapicos_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .option("encoding", "UTF-8")
            .csv(fitoterapicos_path)
        )

        return cls(medicamentos_df, fitoterapicos_df)

    @classmethod
    def from_s3(
        cls,
        spark: SparkSession,
        stage: str,
    ) -> "AnvisaLmipParser":
        """
        Cria o parser a partir de arquivos CSV no S3.

        Args:
            spark: SparkSession
            stage: Ambiente (dev ou prod)

        Returns:
            Instância do parser com os DataFrames carregados e processados

        """
        s3_bucket = f"maggu-datalake-{stage}"
        medicamentos_path = f"s3://{s3_bucket}/0-raw-layer/anvisa/lmip/anexo1.csv"
        fitoterapicos_path = f"s3://{s3_bucket}/0-raw-layer/anvisa/lmip/anexo2.csv"

        return cls.from_csv(spark, medicamentos_path, fitoterapicos_path)
