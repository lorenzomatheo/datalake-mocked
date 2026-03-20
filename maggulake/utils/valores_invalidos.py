import re

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame

LITERAIS_INVALIDOS: list[str] = [
    "n/a",
    "n/i",
    "n/d",
    "n.a",
    "n.a.",
    "n.i",
    "n.i.",
    "n.d",
    "n.d.",
    "na",
    "ni",
    "nd",
    "none",
    "null",
    "nan",
    "-",
    "--",
    "---",
    ".",
    "..",
    "...",
]

COLUNAS_PERMITIR_NUMERICO: list[str] = [
    "ean",
    "numero_registro",
    "dosagem",
    "volume_quantidade",
    "tamanho_produto",
]

COLUNAS_EXCLUIR: list[str] = [
    "id",
    "ean",
    "nome",
    "imagem_url",
    "informacoes_para_embeddings",
    "bula",
    "fonte",
    "lojas_com_produto",
    "status",
]

_RE_SO_PONTUACAO = re.compile(r"^[\W_]+$")
_RE_CARACTERE_REPETIDO = re.compile(r"^(.)\1*$")
_RE_SO_DIGITOS = re.compile(r"^\d+$")


def valor_eh_invalido(valor: str | None, permitir_numerico: bool = False) -> bool:
    if valor is None:
        return True

    limpo = valor.strip()
    if not limpo:
        return True

    minusculo = limpo.lower()

    if (
        minusculo in LITERAIS_INVALIDOS
        or _RE_SO_PONTUACAO.match(limpo)
        or len(limpo) == 1
    ):
        return True

    eh_digito = _RE_SO_DIGITOS.match(limpo)

    if len(limpo) > 1 and _RE_CARACTERE_REPETIDO.match(minusculo):
        if not (permitir_numerico and eh_digito):
            return True

    if not permitir_numerico and eh_digito:
        return True

    return False


def nullifica_coluna_se_valor_invalido(
    coluna: Column | str,
    permitir_numerico: bool = False,
    tipo: T.DataType | None = None,
) -> Column:
    col_ref = F.col(coluna) if isinstance(coluna, str) else coluna

    if isinstance(tipo, T.ArrayType) and isinstance(tipo.elementType, T.StringType):
        filtrado = F.filter(col_ref, _cria_filtro_elemento(permitir_numerico))
        return F.when(
            filtrado.isNotNull() & (F.size(filtrado) > 0),
            filtrado,
        )

    if tipo is not None and not isinstance(tipo, T.StringType):
        return col_ref

    eh_invalido = _constroi_condicao_invalida(col_ref, permitir_numerico)
    return F.when(~eh_invalido, col_ref)


def _constroi_condicao_invalida(
    coluna: Column,
    permitir_numerico: bool = False,
) -> Column:
    limpo = F.trim(coluna)
    minusculo = F.lower(limpo)
    eh_digito = limpo.rlike(r"^\d+$")

    caractere_repetido = minusculo.rlike(r"^(.)\1*$") & (F.length(limpo) > F.lit(1))

    if permitir_numerico:
        regra_repetido = caractere_repetido & ~eh_digito
    else:
        regra_repetido = caractere_repetido

    eh_invalido = (
        coluna.isNull()
        | (limpo == F.lit(""))
        | minusculo.isin(LITERAIS_INVALIDOS)
        | limpo.rlike(r"^[\W_]+$")
        | regra_repetido
        | (F.length(limpo) == F.lit(1))
    )

    if not permitir_numerico:
        eh_invalido = eh_invalido | eh_digito

    return eh_invalido


def _cria_filtro_elemento(permitir_numerico: bool):
    def _filtrar(elemento: Column) -> Column:
        return ~_constroi_condicao_invalida(elemento, permitir_numerico)

    return _filtrar


def nullifica_valores_invalidos(
    df: DataFrame,
    schema: T.StructType | None = None,
    colunas: list[str] | None = None,
    colunas_excluir: list[str] | None = None,
    colunas_permitir_numerico: list[str] | None = None,
) -> DataFrame:
    excluir = set(colunas_excluir or COLUNAS_EXCLUIR)
    permitir_numerico = set(colunas_permitir_numerico or COLUNAS_PERMITIR_NUMERICO)
    schema_ref = schema or df.schema

    if colunas is not None:
        colunas_alvo = [c for c in colunas if c not in excluir]
    else:
        colunas_alvo = [
            campo.name
            for campo in schema_ref.fields
            if campo.name not in excluir
            and (
                isinstance(campo.dataType, T.StringType)
                or (
                    isinstance(campo.dataType, T.ArrayType)
                    and isinstance(campo.dataType.elementType, T.StringType)
                )
            )
        ]

    for nome_coluna in colunas_alvo:
        tipo_coluna = schema_ref[nome_coluna].dataType
        numerico_permitido = nome_coluna in permitir_numerico
        df = df.withColumn(
            nome_coluna,
            nullifica_coluna_se_valor_invalido(
                nome_coluna, numerico_permitido, tipo=tipo_coluna
            ),
        )

    return df
