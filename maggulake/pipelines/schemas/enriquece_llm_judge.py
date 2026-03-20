from typing import Optional

from pydantic import BaseModel, Field
from pyspark.sql import types as T

# Pydantic Models


class TriagemRapida(BaseModel):
    """Schema leve para a primeira passada de triagem do LLM Judge."""

    ean: str = Field(description="EAN do produto sem nenhuma alteração")
    overall_score: float = Field(description="Pontuação geral de qualidade de 0 a 1")
    is_valid: bool = Field(
        description="Indica se o conteúdo é válido e de alta qualidade, apto para uso."
    )
    campos_com_problema: list[str] = Field(
        description="Lista dos nomes EXATOS dos campos que possuem erros ou estão incompletos. Lista vazia se não houver problemas."
    )


class FieldEvaluation(BaseModel):
    field_name: str = Field(description="Nome do campo avaliado")
    is_complete: bool = Field(description="Indica se o campo está preenchido")
    is_accurate: bool = Field(description="Indica se a informação está correta")
    confidence: float = Field(description="Confiança na avaliação (0-1)")
    issues: list[str] = Field(description="Problemas específicos do campo")


# Define evaluation schema
class QualidadeEnriquecimento(BaseModel):
    # TODO: no geral essas descrições estão bem curtas, entendo que foi feito
    # visando redução de tokens, porém seria interessante expandir a definição
    # do que seria cada campo, dando bons e maus exemplos para guiar melhor a LLM.

    ean: str = Field(description="Ean do produto sem nenhuma alteração")
    is_valid: bool = Field(
        description="Indica se o conteúdo é válido e de alta qualidade, apto para uso."
    )
    overall_score: float = Field(description="Pontuação geral de qualidade de 0 a 1")
    field_evaluations: list[FieldEvaluation] = Field(description="Avaliação por campo")
    nome: Optional[str] = Field(
        default=None, description="Correção sugerida para o campo nome do produto"
    )
    marca: Optional[str] = Field(
        default=None, description="Correção sugerida para o campo marca do produto"
    )
    fabricante: Optional[str] = Field(
        default=None, description="Correção sugerida para o campo fabricante do produto"
    )
    principio_ativo: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo principio_ativo do produto",
    )
    descricao: Optional[str] = Field(
        default=None, description="Correção sugerida para o campo descricao do produto"
    )
    eh_medicamento: Optional[bool] = Field(
        default=None,
        description="Correção sugerida para o campo eh_medicamento do produto",
    )
    tipo_medicamento: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo tipo_medicamento do produto",
    )
    dosagem: Optional[str] = Field(
        default=None, description="Correção sugerida para o campo dosagem do produto"
    )
    variantes: Optional[list[str]] = Field(
        default=None, description="Correção sugerida para o campo variantes do produto"
    )
    eh_tarjado: Optional[bool] = Field(
        default=None, description="Correção sugerida para o campo eh_tarjado do produto"
    )
    tipo_de_receita_completo: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo tipo_de_receita_completo do produto",
    )
    classes_terapeuticas: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo classes_terapeuticas do produto",
    )
    especialidades: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo especialidades do produto",
    )
    nome_comercial: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo nome_comercial do produto",
    )
    tipo_receita: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo tipo_receita do produto",
    )
    indicacao: Optional[str] = Field(
        default=None, description="Correção sugerida para o campo indicacao do produto"
    )
    eh_otc: Optional[bool] = Field(
        default=None, description="Correção sugerida para o campo eh_otc do produto"
    )
    contraindicacoes: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo contraindicacoes do produto",
    )
    efeitos_colaterais: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo efeitos_colaterais do produto",
    )
    instrucoes_de_uso: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo instrucoes_de_uso do produto",
    )
    advertencias_e_precaucoes: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo advertencias_e_precaucoes do produto",
    )
    interacoes_medicamentosas: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo interacoes_medicamentosas do produto",
    )
    condicoes_de_armazenamento: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo condicoes_de_armazenamento do produto",
    )
    validade_apos_abertura: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo validade_apos_abertura do produto",
    )
    eh_controlado: Optional[bool] = Field(
        default=None,
        description="Correção sugerida para o campo eh_controlado do produto",
    )
    tipos_receita: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo tipos_receita do produto",
    )
    via_administracao: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo via_administracao do produto",
    )
    forma_farmaceutica: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo forma_farmaceutica do produto",
    )
    volume_quantidade: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo volume_quantidade do produto",
    )
    idade_recomendada: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo idade_recomendada do produto",
    )
    sexo_recomendado: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo sexo_recomendado do produto",
    )
    power_phrase: Optional[str] = Field(
        default=None,
        description="Correção sugerida para o campo power_phrase do produto",
    )
    tags_complementares: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo tags_complementares do produto",
    )
    tags_potencializam_uso: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo tags_potencializam_uso do produto",
    )
    tags_atenuam_efeitos: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo tags_atenuam_efeitos do produto",
    )
    tags_substitutos: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo tags_substitutos do produto",
    )
    tags_agregadas: Optional[list[str]] = Field(
        default=None,
        description="Correção sugerida para o campo tags_agregadas do produto",
    )


# PySpark Schemas


# Schema para avaliação de campos individuais
field_evaluation_schema = T.StructType(
    [
        T.StructField("field_name", T.StringType(), True),
        T.StructField("is_complete", T.BooleanType(), True),
        T.StructField("is_accurate", T.BooleanType(), True),
        T.StructField("confidence", T.FloatType(), True),
        T.StructField("issues", T.ArrayType(T.StringType()), True),
    ]
)

# Schema completo para qualidade de enriquecimento
qualidade_enriquecimento_schema = T.StructType(
    [
        # Avaliação dos resultados
        T.StructField("ean", T.StringType(), True),
        T.StructField("is_valid", T.BooleanType(), True),
        T.StructField("overall_score", T.FloatType(), True),
        T.StructField("field_evaluations", T.ArrayType(field_evaluation_schema), True),
        # Campos com sugestão de melhoria pela LLM
        T.StructField("nome", T.StringType(), True),
        T.StructField("marca", T.StringType(), True),
        T.StructField("fabricante", T.StringType(), True),
        T.StructField("principio_ativo", T.StringType(), True),
        T.StructField("descricao", T.StringType(), True),
        T.StructField("eh_medicamento", T.BooleanType(), True),
        T.StructField("tipo_medicamento", T.StringType(), True),
        T.StructField("dosagem", T.StringType(), True),
        T.StructField("variantes", T.ArrayType(T.StringType()), True),
        T.StructField("eh_tarjado", T.BooleanType(), True),
        T.StructField("categorias", T.ArrayType(T.StringType()), True),
        T.StructField("tipo_de_receita_completo", T.StringType(), True),
        T.StructField("classes_terapeuticas", T.ArrayType(T.StringType()), True),
        T.StructField("especialidades", T.ArrayType(T.StringType()), True),
        T.StructField("nome_comercial", T.StringType(), True),
        T.StructField("tipo_receita", T.StringType(), True),
        T.StructField("indicacao", T.StringType(), True),
        T.StructField("eh_otc", T.BooleanType(), True),
        T.StructField("contraindicacoes", T.StringType(), True),
        T.StructField("efeitos_colaterais", T.StringType(), True),
        T.StructField("instrucoes_de_uso", T.StringType(), True),
        T.StructField("advertencias_e_precaucoes", T.StringType(), True),
        T.StructField("interacoes_medicamentosas", T.StringType(), True),
        T.StructField("condicoes_de_armazenamento", T.StringType(), True),
        T.StructField("validade_apos_abertura", T.StringType(), True),
        T.StructField("eh_controlado", T.BooleanType(), True),
        T.StructField("tipos_receita", T.StringType(), True),
        T.StructField("via_administracao", T.StringType(), True),
        T.StructField("forma_farmaceutica", T.StringType(), True),
        T.StructField("volume_quantidade", T.StringType(), True),
        T.StructField("idade_recomendada", T.StringType(), True),
        T.StructField("sexo_recomendado", T.StringType(), True),
        T.StructField("power_phrase", T.StringType(), True),
        T.StructField("tags_complementares", T.ArrayType(T.StringType()), True),
        T.StructField("tags_potencializam_uso", T.ArrayType(T.StringType()), True),
        T.StructField("tags_atenuam_efeitos", T.ArrayType(T.StringType()), True),
        T.StructField("tags_substitutos", T.ArrayType(T.StringType()), True),
        T.StructField("tags_agregadas", T.ArrayType(T.StringType()), True),
        T.StructField("data_execucao", T.TimestampType(), True),
    ]
)
