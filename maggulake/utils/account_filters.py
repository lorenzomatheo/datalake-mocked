"""Shared account filter constants used across data adapters."""

import re

# Padrões LIKE (com suporte a wildcards %) para exclusão de lojas/contas.
# Itens sem wildcard são tratados como correspondência exata.
EXCLUDED_ACCOUNT_LIKE_PATTERNS: list[str] = [
    # Exatas
    "toolspharma",
    "automatiza",
    "hos",
    "asasys",
    "bizpik",
    "consys",
    "dream",
    "erp",
    "loadtest",
    # Wildcards
    "%hosmobileecommerce%",
    "%interativapos%",
    "%maggu%",
    "%teste%",
    "%desativada%",
    "%obsoleto%",
    "%homolog%",
    "%prototipo%",
]


def _like_to_regex(pattern: str) -> str:
    """Converte um padrão LIKE (com %) para sub-padrão regex."""
    if pattern.startswith("%") and pattern.endswith("%"):
        return re.escape(pattern[1:-1])
    if pattern.startswith("%"):
        return re.escape(pattern[1:]) + "$"
    if pattern.endswith("%"):
        return "^" + re.escape(pattern[:-1])
    return "^" + re.escape(pattern) + "$"


# Padrão regex equivalente, para uso com REGEXP_CONTAINS (BigQuery).
EXCLUDED_ACCOUNT_REGEXP: str = "|".join(
    _like_to_regex(p) for p in EXCLUDED_ACCOUNT_LIKE_PATTERNS
)

# Array SQL pré-formatado, para uso com LIKE ANY(ARRAY[...]) (PostgreSQL).
EXCLUDED_ACCOUNT_SQL_ARRAY: str = ", ".join(
    f"'{p}'" for p in EXCLUDED_ACCOUNT_LIKE_PATTERNS
)
