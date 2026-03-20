import re

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from maggulake.utils.valores_invalidos import LITERAIS_INVALIDOS

# ---------------------------------------------------------------------------
# Guards — retornam None antes de qualquer transformação
# ---------------------------------------------------------------------------

# Rejeita strings que são literais inválidos conhecidos (none, null, n/a, -, ...)
_INVALID_RE = re.compile(
    r"^(" + "|".join(re.escape(v) for v in LITERAIS_INVALIDOS) + r")$",
    re.IGNORECASE,
)

# Rejeita strings que contêm "None" como parte de número ou unidade
# \b não funciona aqui pois dígitos e letras são ambos \w — usa substring simples
# Ex: "NoneNone", "500None", "Nonemg" — resíduos de fontes com dados ausentes
_NONE_UNIT_RE = re.compile(r"none", re.IGNORECASE)

# Rejeita instruções de uso/posologia — não são valores de concentração
# Ex: "1 comprimido por dia", "2 a 4 comprimidos a cada 8 horas"
# Âncora ^ só atua dentro da alternativa que a contém
_INSTRUCAO_RE = re.compile(
    r"(?:"
    r"^\d+(?:\s+a\s+\d+)?\s+(?:comprimido|cápsula|capsula|sachê|sache|drágea|dragea)s?\b"
    r"|a\s+cada\s+\d+\s+horas?"
    r"|vezes?\s+ao\s+dia"
    r")",
    re.IGNORECASE,
)

# Rejeita notação científica/exponencial e unidades virais especializadas
# Ex: "10^3,0 ccid50", "2,0 x 10e13 gv/ml", "5 x 10^10 partículas virais"
_NOTACAO_ESPECIALIZADA_RE = re.compile(
    r"\b(?:ccid50|gv/ml|gv\b|pfu\b)|"
    r"\d+\^|\bx\s*10[e^]",
    re.IGNORECASE,
)

# Rejeita unidades especializadas de vacinas, homeopatia e veterinária
# que não são concentrações farmacêuticas convencionais
# Ex: "1.350 ufp", "25tru/g", "15 lf", "40 u.d.", "1ch", "1d"
_UNIDADE_NAOPADRAO_RE = re.compile(
    r"\bufp\b"  # unidades formadoras de placa
    r"|\d+\s*tru\b"  # turbidity reducing units: "25tru"
    r"|\d+\s*pcc\b"  # colony forming units: "10pcc", "20pcc"
    r"|\blf\b"  # limite de floculação (vacina)
    r"|\bu\.d\."  # unidades antigênicas: "u.d."
    r"|\d+\s*ch\b"  # homeopático CH: "1ch", "5ch"
    r"|\b\d+\s*d\b",  # homeopático D: "1d", "1d 10%"
    re.IGNORECASE,
)

# Rejeita intervalos e alternativas de dosagem — não é um valor único de concentração
# Ex: "100mg - 150mg", "15-19mg/ml", "100mg ou 1g", "0.1 a 0.25 g/kg", "100mg, 200mg"
# Nota: \b falha entre dígito e letra (ambos \w), por isso usamos \s+-\s+ para hífen com espaços
# e \d+-\d+ para hífen compacto (dígitos diretamente adjacentes, ex: "15-19")
# A vírgula usa (?!\d*h\b) para não confundir "150mg,12h" (sufixo temporal) com lista
_INTERVALO_RE = re.compile(
    r"\s+-\s+"  # hífen com espaços: "100mg - 150mg"
    r"|\d+-\d+"  # hífen compacto: "15-19mg/ml"
    r"|\s+ou\s+"  # alternativa "ou": "100mg ou 1g"
    r"|\s+a\s+\d"  # range "a": "0.1 a 0.25 g/kg"
    r"|(?:mg|g|mcg|µg|ml|ui|iu|%)\s*,\s*(?!\d*h\b)\d",  # lista (exceto sufixo temporal ,12h)
    re.IGNORECASE,
)

# Rejeita frações e denominadores múltiplos sem unidade no numerador
# Ex: "20/30mg", "100/25mg", "0/0.5/2.5 g/g"
_FRACAO_COMPLEXA_RE = re.compile(r"\b\d+\s*/\s*\d", re.IGNORECASE)

# Rejeita razões adimensionais (mesma unidade nos dois lados do /)
# \b omitido no início — falha quando a unidade é precedida por dígito (ambos \w)
# Ex: "0,07ml/ml" → None, "0,4mg/mg" → None
# Preserva: "0.5mg/g" (unidades diferentes)
_ADIMENSIONAL_RE = re.compile(
    r"(?:ml|mL)/(?:ml|mL)\b"
    r"|mg/mg\b",
    re.IGNORECASE,
)

# Rejeita valores que misturam g e mg como unidades independentes (escalas inconsistentes)
# \b omitido no início — falha entre dígito e letra (ex: "10mg", ambos \w)
# Usa (?<![mc/]) para não rejeitar mg/g (unidade de concentração legítima)
# Ex: "10mg + 3,5g + 12g" → None
# Preserva: "0.5mg/g + 1mg/g" (mg/g é unidade válida, /g excluído pelo lookbehind)
_MISTURA_ESCALA_RE = re.compile(
    r"mg\b.*\+.*(?<![a-zA-Z/])g\b"
    r"|(?<![a-zA-Z/])g\b.*\+.*mg\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Transformações — aplicadas em ordem na função
# ---------------------------------------------------------------------------

# Remove prefixos de comparação — mantém o valor numérico
# Ex: "≥ 1.000ui" → "1.000ui", "> 20 ui" → "20 ui"
_COMPARACAO_RE = re.compile(r"^[≥>]\s*")

# Remove separador de milhar apenas nos casos inequivocamente milhares, para não
#   – múltiplos grupos .YYY:  "1.000.000ui" → "1000000ui"  (impossível ser decimal)
#   – único grupo .000:       "1.000mg", "100.000ui" → ponto decimal "1.000" = 1.0 não existe
# Casos ambíguos como "1.250" ou "10.250" são preservados — podem ser decimais de ponto.
_MILHAR_RE = re.compile(
    r"([1-9]\d{0,2})((?:\.\d{3}){2,})(?=\D|$)"  # múltiplos grupos: 1.000.000
    r"|([1-9]\d{0,2})(\.000)(?=\D|$)",  # único grupo .000: 1.000, 100.000
)

# Normaliza aliases de unidades para forma canônica
# Usa lookbehind (?<=[0-9]) pois \b falha quando dígito precede letra diretamente
# Ex: "200ug" → "200mcg", "200µg" → "200mcg", "200 µg" → "200mcg"
_UNIT_ALIAS_RE = re.compile(r"(?<=[0-9])\s*(?:µg|μg|ug)(?!\w)", re.IGNORECASE)

# Normaliza "u" isolado para "ui" (International Units)
# Usa \s* para capturar com ou sem espaço: "100 u" → "100ui", "10000u" → "10000ui"
# (?!i) evita processar "ui" já correto
_UNIT_U_BARE_RE = re.compile(r"(?<=[0-9])\s*u\b(?!i)", re.IGNORECASE)

# Remove sufixos de frequência/tempo — não fazem parte da concentração
# Ex: "10mcg/h" → "10mcg", "13.3mg/24h" → "13.3mg",
#     "150mg,12h" → "150mg", "9.0mg (4.6mg/24h)" → "9.0mg"
_TEMPORAL_RE = re.compile(
    r"\s*\([^)]*\d+\s*h[^)]*\)|[,/]\s*\d*\s*h\b.*$",
    re.IGNORECASE,
)

# Distribui unidade posicionada fora de parênteses a cada componente
# Ex: "(1000 + 200)mg" → "1000mg + 200mg", "(120+120+120)mcg/ml" → "120mcg/ml + ..."
_PAREN_UNIDADE_FORA_RE = re.compile(
    r"\(\s*(\d[^)]*?)\s*\)\s*((?:mg|g|mcg|µg|mui|ui|iu|meq|mmol|ud|mL|ml|%)(?:/\w+)?)",
    re.IGNORECASE,
)

# Remove parentéticos descritivos (cor, tipo, identificador de ampola/drágea)
# Ex: "(com branco)", "(drag a)", "(ampola 1)", "(com azul)"
_PARENTETICO_DESCRITIVO_RE = re.compile(
    r"\s*\(\s*(?:com\s+\w+|drag[aá]?(?:ea)?\s*\w*|ampola\s+\d+)\s*\)",
    re.IGNORECASE,
)

# Remove sufixos descritivos de unidade posológica após a concentração
# Ex: "500mg por comprimido" → "500mg", "500mg para cada cepa" → "500mg"
#     "0.25mg por quilo de peso corpóreo" → "0.25mg"
_POR_SUFFIX_RE = re.compile(
    r"\s+(?:"
    r"por\s+(?:capsula|cápsula|comprimido|dose|ml|ampola|frasco|sachê|sache"
    r"|supositorio|supositório|porção|porcao|dia|gota|quilo|kg(?:\s+de\s+\w+)*)"
    r"|para\s+cada\s+\w+"
    r")\b.*$",
    re.IGNORECASE,
)

# Remove denominadores de dispensação não-farmacêuticos após "/"
# Ex: "0.21mg/gota" → "0.21mg", "15mcg/cepa" → "15mcg"
_DENOMINADOR_SLASH_RE = re.compile(r"/\s*(?:gota|cepa)\b.*$", re.IGNORECASE)

# Remove descritores de ingrediente embutidos ("de X")
# Para no "+" para preservar valores compostos
# Ex: "1g de vitamina c" → "1g", "200mg de cafeína + 300mg de taurina" → "200mg + 300mg"
_DE_INGREDIENTE_RE = re.compile(r"\s+de\s+[^+]+", re.IGNORECASE)

# Remove espaço entre número e unidade de medida
# Ex: "500 mg" → "500mg", "25 mg/mL" → "25mg/mL"
_RE_NUM_UNIDADE = re.compile(
    r"(\d)\s+(mg|g|mcg|µg|mui|ui|iu|meq|mmol|ud|mL|ml|%)",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Extração do nome do produto
# ---------------------------------------------------------------------------

# Padrão de dosagem para extração a partir do nome do produto
# Baseado nos padrões de remoção de dosagem de padroniza_principio_ativo.py (linhas 98-104)
# 'g' excluído da unidade principal — evita confusão com peso de embalagem (ex: "Pomada 10g")
_RE_DOSAGEM = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*"
    r"(mg|mcg|µg|ui|iu|meq|%)\s*"
    r"(?:/\s*(\d+(?:[.,]\d+)?\s*)?(mL|ml|g|kg|dose|comp|cp))?",
    re.IGNORECASE,
)


_GUARDS = [
    _INSTRUCAO_RE,
    _NOTACAO_ESPECIALIZADA_RE,
    _UNIDADE_NAOPADRAO_RE,
    _INTERVALO_RE,
    _FRACAO_COMPLEXA_RE,
    _ADIMENSIONAL_RE,
    _MISTURA_ESCALA_RE,
]


def _e_dosagem_invalida(text: str) -> bool:
    if _INVALID_RE.match(text) or _NONE_UNIT_RE.search(text):
        return True
    return any(r.search(text) for r in _GUARDS)


def normaliza_dosagem(raw: str | None) -> str | None:
    """Padroniza o formato de uma string de dosagem.

    Exemplos:
        "500 mg"                           → "500mg"
        "12,5mg"                           → "12.5mg"
        "1.000mg"                          → "1000mg"
        "≥ 1.000ui"                        → "1000ui"
        "200ug"                            → "200mcg"
        "10000u"                           → "10000ui"
        "10mcg/h"                          → "10mcg"
        "13,3mg/24h"                       → "13.3mg"
        "9,0mg (4,6mg/24h)"               → "9.0mg"
        "(1000 + 200)mg"                   → "1000mg + 200mg"
        "(0,05mg + 0,03mg) + (0,075mg...)" → "0.05mg + 0.03mg + 0.075mg..."
        "0,05mg + 0,03mg (drag a)"         → "0.05mg + 0.03mg"
        "500mg por comprimido"             → "500mg"
        "0,21mg/gota"                      → "0.21mg"
        "1g de vitamina c por cápsula"     → "1g"
        "1 comprimido por dia"             → None
        "1ch"                              → None
        "100mg - 150mg"                    → None
        "20/30mg"                          → None
        "0,07ml/ml"                        → None
        "10mg + 3,5g"                      → None
        None / ""                          → None
    """
    if not raw or not raw.strip():
        return None
    text = raw.strip()
    if _e_dosagem_invalida(text):
        return None
    # remove prefixo de comparação: "≥ 1.000ui" → "1.000ui"
    text = _COMPARACAO_RE.sub("", text)
    # separador de milhar → remove pontos antes de converter decimal
    text = _MILHAR_RE.sub(lambda m: m.group(0).replace(".", ""), text)
    # vírgula decimal → ponto: "12,5mg" → "12.5mg"
    text = re.sub(r"(\d),(\d)", r"\1.\2", text)
    # aliases de unidade → forma canônica: "µg"/"ug" → "mcg"
    text = _UNIT_ALIAS_RE.sub("mcg", text)
    # "u" isolado → "ui": "100 u" → "100ui", "10000u" → "10000ui"
    text = _UNIT_U_BARE_RE.sub("ui", text)
    # sufixos temporais: "/h", "/24h", ",12h", "(4,6mg/24h)"
    text = _TEMPORAL_RE.sub("", text)
    # distribui unidade fora de parênteses: "(1000 + 200)mg" → "1000mg + 200mg"
    text = _PAREN_UNIDADE_FORA_RE.sub(
        lambda m: " + ".join(
            f"{p.strip()}{m.group(2)}"
            for p in re.split(r"\s*\+\s*", m.group(1))
            if p.strip()
        ),
        text,
    )
    # remove parentéticos descritivos: "(drag a)", "(com branco)", "(ampola 1)"
    text = _PARENTETICO_DESCRITIVO_RE.sub("", text)
    # remove parênteses de agrupamento, preservando conteúdo
    text = re.sub(r"\(([^)]+)\)", r"\1", text)
    # sufixos posológicos: "por comprimido", "para cada cepa", "por quilo de peso"
    text = _POR_SUFFIX_RE.sub("", text)
    # denominadores de dispensação: "/gota", "/cepa"
    text = _DENOMINADOR_SLASH_RE.sub("", text)
    # descritores de ingrediente: "de vitamina c", "de cafeína"
    text = _DE_INGREDIENTE_RE.sub("", text)
    text = _RE_NUM_UNIDADE.sub(r"\1\2", text)  # remove espaço número-unidade
    text = re.sub(r"\s*\+\s*", " + ", text)  # separador uniforme
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def extrai_dosagem_do_nome(nome: str | None) -> str | None:
    """Extrai a dosagem a partir do nome do produto como fallback.

    Captura a primeira ocorrência de um padrão de concentração no nome.

    Exemplos:
        "Dipirona 500mg Comprimido"     → "500mg"
        "Amoxicilina 500mg/5mL Susp"   → "500mg/5mL"
        "Hidrocortisona 1% Creme"       → "1%"
        "Insulina 100 UI/mL"            → "100UI/mL"
        "Shampoo Dove 400ml"            → None  (ml sem unidade de fármaco)
        "Pomada 10g"                    → None  (g excluído da unidade principal)
    """
    if not nome or not nome.strip():
        return None
    m = _RE_DOSAGEM.search(nome)
    if not m:
        return None
    quantidade = m.group(1)
    unidade = m.group(2)
    denominador_qtd = (m.group(3) or "").strip()
    denominador_unidade = m.group(4) or ""
    dosagem = f"{quantidade}{unidade}"
    if denominador_unidade:
        dosagem += f"/{denominador_qtd}{denominador_unidade}"
    return dosagem


normaliza_dosagem_udf = F.udf(normaliza_dosagem, StringType())
extrai_dosagem_do_nome_udf = F.udf(extrai_dosagem_do_nome, StringType())
