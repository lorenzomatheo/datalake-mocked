"""Here we provide functions for string manipulations"""

import re
import unicodedata
from typing import Any, Iterator, Optional

import numpy as np
import tiktoken
from unidecode import unidecode


def converte_array_para_string(array: Iterator[str]) -> Optional[str]:
    """Transforma um array em uma string, separando os elementos por '|'."""
    return '|'.join(array) if array else None


def truncate_text_to_tokens(
    text: str, max_length: int, embedding_encoding: str = "cl100k_base"
) -> str:
    encoding = tiktoken.get_encoding(embedding_encoding)
    tokens = encoding.encode(text)

    if len(tokens) > max_length:
        truncated_text = encoding.decode(tokens[:max_length])
        return truncated_text
    else:
        return text


def remove_accents(input_str: str) -> str:
    """This function receives a string and removes all accents from it. In case
    it is possible, the code also replaces the accented character with the
    non-accented version of it. This is quite useful when working with
    non-english languages.
    """
    if input_str is None:
        return ""
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def sanitize_string(input_str: str) -> str:
    """This function removes all accents from a string and also removes all
    special characters from it. It also converts the string to lower case and
    replaces spaces with underscores.
    """
    # First, we'll remove the accents using our previous function
    no_accents = remove_accents(input_str)

    # Convert to lowercase
    lowercase = no_accents.lower()

    # Replace spaces with underscores
    with_underscores = re.sub(r"\s+", "_", lowercase)

    # Now, let's remove all special characters except for underscores
    cleaned = re.sub(r"[^\w_]", "", with_underscores)

    return cleaned


def standardize_phone_number(phone_number: str) -> str:
    """Use this function to standardize phone numbers. It will return a string
    with the phone number in the format '(XX) XXXXX-XXXX' or 'XXXX-XXXX' if
    the number doesn't include a DDD.

    Parameters
    ----------
    phone_number : str
        The phone number to be standardized

    Returns
    -------
    str
        The standardized phone number
    """
    # Remove all non-digit characters
    digits_only = re.sub(r"\D", "", str(phone_number))

    # Check if the number starts with '0' and remove it
    if digits_only.startswith("0"):
        digits_only = digits_only[1:]

    # Check the length of the digits
    if len(digits_only) == 8:
        # If it has 8 digits, assume it's a landline without DDD
        return f"{digits_only[:4]}-{digits_only[4:]}"
    elif len(digits_only) == 9 and digits_only[0] != "0":
        # If it has 9 digits and doesn't start with '0', assume it's a cell phone without DDD
        return f"{digits_only[:5]}-{digits_only[5:]}"
    elif len(digits_only) == 9:
        # If it has 9 digits and starts with '0', assume it includes a 2-digit DDD
        return f"({digits_only[:2]}) {digits_only[2:5]}-{digits_only[5:]}"
    elif len(digits_only) == 10:
        # If it has 10 digits, assume it includes a 2-digit DDD
        return f"({digits_only[:2]}) {digits_only[2:6]}-{digits_only[6:]}"
    elif len(digits_only) == 11:
        # If it has 11 digits, assume it includes a 2-digit DDD plus a leading '9' for cell phones

        return f"({digits_only[:2]}) {digits_only[2:7]}-{digits_only[7:]}"
    else:
        return None  # Invalid phone number


# TODO: adicionar type hints em clean_string, list_without_blanks e to_list_or_null.
def clean_string(string):
    return string and string.strip() or None


def list_without_blanks(list_of_str: list[str]):
    return [c for i in list_of_str if (c := clean_string(i))]


def to_list_or_null(string):
    clean = clean_string(string)

    if not clean:
        return None

    return list_without_blanks(clean.split("|"))


def sanitize_string_sql(value: str) -> str:
    """Sanitizar strings para evitar SQL injection"""
    if not value or value in ["None", "null", "NULL", "NaN"]:
        return ""
    return re.sub(r"[^\w\s,.@-]", "", value).strip()


def concat_categories(x: list | np.ndarray | Any) -> str:
    if isinstance(x, (list, np.ndarray)):
        return '|'.join(str(item) for item in x if item is not None)
    return ''


def remove_special_characters(x: str) -> str:
    if not isinstance(x, str):
        x = str(x)
    # Remove todos os caracteres que não sejam alfanuméricos
    x = re.sub(r'[^a-zA-Z0-9\s\.\-\_]', '', x)
    # Substitui espaços em branco por um único espaço
    x = re.sub(r'\s+', ' ', x)
    return x.strip()


def limpa_cnpj(cnpj: str) -> str:
    if not isinstance(cnpj, str):
        cnpj = str(cnpj)
    return (
        cnpj.replace(".", "")
        .replace("-", "")
        .replace("/", "")
        .replace("\n", "")
        .lstrip("0")
        .strip()
    )


def validate_email_return_none_if_fail(email: str | None) -> str | None:
    """Valida o email, retornando None se o email for inválido"""

    if email is None:
        return None

    email_limpo = email.strip()

    if not email_limpo:
        return None

    if '@' not in email_limpo:
        return None

    if "." not in email_limpo:
        return None

    # se o ponto não vier depois do @, é inválido
    pos_arroba = email_limpo.index('@')
    pos_ultimo_ponto = email_limpo.rindex('.')
    if pos_ultimo_ponto < pos_arroba:
        return None

    return email_limpo


def normalize_text_alphanumeric(text: str | None) -> str | None:
    """
    Normaliza texto para comparação: remove acentos, converte para lowercase,
    e mantém apenas caracteres alfanuméricos.

    Útil para comparação de nomes de produtos, marcas e fabricantes onde
    pontuação e espaços não devem afetar a comparação.

    Args:
        text: String para normalizar. Pode ser None.

    Returns:
        String normalizada contendo apenas caracteres alfanuméricos em lowercase,
        ou None se o input for None.

    Examples:
        >>> normalize_text_alphanumeric("Aspirina® 500mg")
        'aspirina500mg'
        >>> normalize_text_alphanumeric("Água Sanitária - 1L")
        'aguasanitaria1l'
        >>> normalize_text_alphanumeric(None)
        None
    """
    if text is None:
        return None

    # Remove espaços extras e converte para lowercase
    normalized = text.strip().lower()

    # Remove acentos usando normalização NFKD
    normalized = unicodedata.normalize('NFKD', normalized)
    normalized = ''.join(c for c in normalized if not unicodedata.combining(c))

    # Remove tudo que não for alfanumérico
    normalized = re.sub(r"[^a-z0-9]+", "", normalized)

    return normalized


def string_to_column_name(col: str) -> str:
    col = col.lower().strip()
    espacado = re.sub(r"[ ,;\n\t=]", "_", col)
    sem_outros = re.sub(r"[{}()]", "", espacado)

    return unidecode(sem_outros)
