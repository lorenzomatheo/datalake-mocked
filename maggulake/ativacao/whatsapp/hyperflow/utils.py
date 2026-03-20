import re

from maggulake.ativacao.whatsapp.exceptions import NumeroTelefoneInvalidoError


def padroniza_numero_whatsapp(numero: str) -> str:
    """Padroniza de um jeito que o HYPERFLOW consegue entender"""

    if not isinstance(numero, str):
        raise TypeError("Número deve ser uma string.")

    # Remove espaços, traços, parênteses, sinais de + e outros caracteres não numéricos
    new = re.sub(r"[ \-()+]+", "", numero)

    # Remove caracteres em branco no início e no fim
    new = new.strip()

    # Se o numero eh BR, nao precisa do codigo do pais
    if new.startswith("55") and len(new) > 11:
        new = new[2:]

    # Números proibidos: sequência do mesmo dígito (ex: 00000000, 55555555, etc)
    if re.fullmatch(r"(\d)\1{7,}", new):
        raise NumeroTelefoneInvalidoError(
            "Número inválido: não pode ser uma sequência do mesmo dígito."
        )

    # Validação: número BR deve ter pelo menos 10 dígitos (2 DDD + 8 número)
    if len(new) < 10:
        raise NumeroTelefoneInvalidoError(
            "Número inválido: deve conter pelo menos 10 dígitos (2 DDD + 8 número) ou 11 dígitos (2 DDD + 9 número)."
        )

    return new
