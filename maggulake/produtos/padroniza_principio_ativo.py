import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from maggulake.enums.formas_sal import FormasSal
from maggulake.utils.strings import remove_accents

_FORMAS_SAL = FormasSal.list()
_FORMAS_SAL_PATTERN = "|".join(re.escape(s) for s in _FORMAS_SAL)

MAPA_HIDRATACAO: dict[str, str] = {
    "triidratado": "tri-hidratado",
    "trihidratado": "tri-hidratado",
    "triidratada": "tri-hidratada",
    "trihidratada": "tri-hidratada",
    "diidratado": "di-hidratado",
    "dihidratado": "di-hidratado",
    "diidratada": "di-hidratada",
    "dihidratada": "di-hidratada",
    "monohidratado": "monoidratado",
    "mono-hidratado": "monoidratado",
    "monohidratada": "monoidratada",
    "mono-hidratada": "monoidratada",
    "hemihidratado": "hemi-hidratado",
    "hemiidratado": "hemi-hidratado",
    "heptahidratado": "heptaidratado",
    "hepta-hidratado": "heptaidratado",
    "hexahidratado": "hexaidratado",
    "hexa-hidratado": "hexaidratado",
    "hemipentahidratado": "hemipentaidratado",
    "hemi-pentaidratado": "hemipentaidratado",
    "pentahidratado": "pentaidratado",
    "penta-hidratado": "pentaidratado",
}


def padronizar_principio_ativo(raw: str | None) -> str | None:
    if not raw or not raw.strip():
        return raw

    text = raw.strip().lower()

    # Normaliza quebras de linha (\r\n, \r, \n) para separador " + "
    text = text.replace("\r\n", " + ").replace("\r", " + ").replace("\n", " + ")

    # Ponto-e-vírgula como separador de componentes
    text = text.replace(";", " + ")

    # Vírgula seguida de letra indica novo componente (ex: "paracetamol, cafeina")
    # Não captura vírgulas de dosagem como "0,12%"
    text = re.sub(r",\s*(?=[a-zA-ZÀ-ÿ])", " + ", text)

    # Colapsa espaços múltiplos em um só
    text = re.sub(r"\s+", " ", text).strip()

    components = [c.strip() for c in text.split("+") if c.strip()]

    normalized = []
    for comp in components:
        # Remove acentos para chave canônica (ex: "ácido" -> "acido")
        c = remove_accents(comp.strip())
        c = re.sub(r"\s+", " ", c)

        # Corrige variantes ortográficas de hidratação
        # (ex: "triidratado" e "trihidratado" -> "tri-hidratado")
        for old, new in MAPA_HIDRATACAO.items():
            c = c.replace(old, new)

        # Padrão "substância (sal)" -> "sal de substância"
        # Ex: "metformina (cloridrato)" -> "cloridrato de metformina"
        pm = re.match(r"^(.+?)\s*\((" + _FORMAS_SAL_PATTERN + r")\)\s*$", c)
        if pm:
            c = f"{pm.group(2)} de {pm.group(1).strip()}"

        # Padrão "substância sal" (pós-fixo) -> "sal de substância"
        # Ex: "metformina cloridrato" -> "cloridrato de metformina"
        words = c.split()
        if len(words) >= 2 and words[-1] in _FORMAS_SAL and words[-2] != "de":
            c = f"{words[-1]} de {' '.join(words[:-1])}"

        # Padrão "sal substância" (pré-fixo sem 'de') -> "sal de substância"
        # Ex: "cloridrato metformina" -> "cloridrato de metformina"
        for salt in _FORMAS_SAL:
            m = re.match(r"^(" + re.escape(salt) + r")\s+(?!de\b)(\w.*)$", c)
            if m:
                c = f"{m.group(1)} de {m.group(2)}"
                break

        # Inverte "X ácido" -> "ácido X" (DCB usa prefixo)
        # Ex: "acetilsalicílico acido" -> "acido acetilsalicílico"
        words = c.split()
        if "acido" in words and not c.startswith("acido"):
            idx = words.index("acido")
            c = "acido " + " ".join(words[:idx] + words[idx + 1 :])

        # Remove dosagem entre parênteses no final: "(500 mg)"
        c = re.sub(r"\s*\(\d+\s*mg\)\s*$", "", c)

        # Remove concentração percentual no final: "0,12%"
        c = re.sub(r"\s+\d+([.,]\d+)?%\s*$", "", c)

        # Remove dosagem mg ou mg/ml no final: "500mg", "500mg/ml"
        c = re.sub(r"\s+\d+([.,]\d+)?\s*mg(/ml)?\s*$", "", c)

        # Remove parênteses residuais não-sal no final: "(comp rosado)", "(1%)"
        c = re.sub(r"\s*\([^)]*\)\s*$", "", c)

        normalized.append(c.strip())

    # Ordena alfabeticamente e remove duplicatas
    normalized = sorted(set(normalized))
    return " + ".join(normalized).upper()


padronizar_principio_ativo_udf = F.udf(padronizar_principio_ativo, StringType())
