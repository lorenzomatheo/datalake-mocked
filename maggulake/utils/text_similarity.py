import re

from maggulake.utils.strings import remove_accents


def extrair_tokens_significativos(texto: str | None) -> set[str]:
    """
    Extrai tokens significativos de um texto, removendo palavras não-informativas.

    Remove automaticamente:
    - Números isolados (100, 50, 2000)
    - Unidades de medida comuns (mg, ml, g, ui, cp, caps, etc.)
    - Números combinados com unidades (50mg, 2000ui, 100ml)
    - Pontuação e caracteres especiais
    - Palavras muito curtas (< 3 caracteres)
    - Palavras genéricas comuns (com, sem, para, por, etc.)

    Args:
        texto: String para extrair tokens. Pode ser None.

    Returns:
        Conjunto de tokens significativos em lowercase.
        Retorna conjunto vazio se input for None ou sem tokens válidos.
    """
    if not texto:
        return set()

    # Normaliza usando função compartilhada de strings.py
    texto = remove_accents(str(texto)).lower()

    # Extrai palavras (remove pontu ação automaticamente)
    tokens = set(re.findall(r'\w+', texto))

    # Define unidades de medida e palavras comuns a filtrar
    palavras_filtradas = {
        # Unidades de medida
        'mg',
        'ml',
        'mcg',
        'ui',
        'iu',
        'gr',
        # Unidades de quantidade
        'un',
        'unid',
        'und',
        'cp',
        'cps',
        'cap',
        'caps',
        'com',
        'comp',
        'cx',
        'fr',
        'po',
        'tab',
        'pct',
        'env',
        'sache',
        'ampola',
        'amp',
        # Palavras comuns genéricas
        'sem',
        'para',
        'por',
        'ate',
        'dos',
        'das',
        'del',
    }

    tokens_significativos = set()

    for token in tokens:
        # Remove se for apenas números
        if re.match(r'^\d+$', token):
            continue

        # Remove se for número + letras (ex: 50mg, 2000ui, 100g)
        if re.match(r'^\d+[a-z]+$', token):
            continue

        # Remove se for letra + número (ex: g100, ml500)
        if re.match(r'^[a-z]+\d+$', token):
            continue

        # Remove se estiver na lista de palavras filtradas
        if token in palavras_filtradas:
            continue

        # Remove tokens muito curtos (< 3 caracteres)
        # Exceção: mantém se tiver apenas letras maiúsculas no original (siglas)
        if len(token) < 3:
            continue

        # Adiciona token significativo
        tokens_significativos.add(token)

    return tokens_significativos


def calcular_jaccard_similarity(tokens_a: set[str], tokens_b: set[str]) -> float:
    """
    Calcula Jaccard Similarity entre dois conjuntos de tokens.

    Fórmula: J(A, B) = |A ∩ B| / |A ∪ B|
    - Interseção: elementos presentes em ambos os conjuntos
    - União: todos os elementos únicos dos dois conjuntos

    Valores de retorno:
    - 0.0: Conjuntos completamente diferentes (sem elementos em comum)
    - 1.0: Conjuntos idênticos (todos os elementos em comum)
    - 0.0-1.0: Grau de similaridade parcial

    Args:
        tokens_a: Primeiro conjunto de tokens
        tokens_b: Segundo conjunto de tokens

    Returns:
        Similaridade entre 0.0 e 1.0
    """
    if not tokens_a or not tokens_b:
        return 0.0

    intersecao = len(tokens_a.intersection(tokens_b))
    uniao = len(tokens_a.union(tokens_b))

    return intersecao / uniao if uniao > 0 else 0.0


def tokenize_for_matching(
    nome: str,
    principio_ativo: str,
    tamanho_minimo: int,
    termos_blocklist: list[str],
    max_termos_nome: int,
    max_termos_pa: int,
):
    """
    Tokeniza nome e princípio ativo para matching
    Limita aos primeiros N termos para maior precisão
    """
    tokens = []  # Mudança 1: usar lista para manter ordem

    # Processar nome - LIMITAR aos primeiros max_termos_nome
    if nome:
        nome_limpo = remove_accents(nome).upper().strip()
        palavras = re.findall(r'\b\w+\b', nome_limpo)

        termos_adicionados = 0
        for palavra in palavras:
            if termos_adicionados >= max_termos_nome:  # Mudança 2: limitar quantidade
                break

            palavra_alpha = re.sub(r'[^A-Z]', '', palavra)
            if (
                len(palavra_alpha) >= tamanho_minimo
                and palavra_alpha not in termos_blocklist
            ):
                if palavra_alpha not in tokens:  # Evitar duplicatas
                    tokens.append(palavra_alpha)
                    termos_adicionados += 1

    # Processar princípio ativo - LIMITAR aos primeiros max_termos_pa
    if principio_ativo:
        pa_limpo = remove_accents(principio_ativo).upper().strip()
        palavras_pa = re.findall(r'\b\w+\b', pa_limpo)

        termos_adicionados_pa = 0
        for palavra in palavras_pa:
            if termos_adicionados_pa >= max_termos_pa:  # Mudança 3: limitar PA também
                break

            palavra_alpha = re.sub(r'[^A-Z]', '', palavra)
            if (
                len(palavra_alpha) >= tamanho_minimo
                and palavra_alpha not in termos_blocklist
            ):
                if palavra_alpha not in tokens:
                    tokens.append(palavra_alpha)
                    termos_adicionados_pa += 1

    return tokens


def filtrar_nomes_por_semelhanca(
    texto_referencia: str | None,
    lista_textos: list[str] | None,
    threshold_minimo: float = 0.20,
    threshold_maximo: float = 0.95,
) -> list[str] | None:
    """
    Filtra lista de strings mantendo apenas aquelas com semelhança adequada.

    Implementa filtro duplo baseado em similaridade semântica:

    Remove strings que sejam:
    1. **Idênticas ou duplicatas** - Similaridade >= threshold_maximo (default 95%)
       Exemplo: "Produto ABC" vs "PRODUTO ABC" → Remove

    2. **Completamente diferentes** - Similaridade < threshold_minimo (default 20%)
       Exemplo: "DUZIMICIN" vs "PAROXETINA" → Remove

    Mantém strings com **semelhança parcial**:
    - Variações relevantes do mesmo produto
    - Nomes alternativos com informação adicional
    - Threshold: threshold_minimo <= similaridade < threshold_maximo

    Cálculo de similaridade:
    - Usa Jaccard Similarity sobre tokens significativos
    - Ignora números, unidades de medida, pontuação
    - Foca em palavras com conteúdo semântico relevante

    Args:
        texto_referencia: String de referência para comparação
        lista_textos: Lista de strings para filtrar
        threshold_minimo: Semelhança mínima para manter (default: 0.20 = 20%)
        threshold_maximo: Semelhança máxima antes de considerar duplicata (default: 0.95 = 95%)

    Returns:
        Lista filtrada de strings com semelhança adequada.
        Retorna None se lista_textos for None ou vazia após filtragem.
        Retorna lista_textos original se texto_referencia for None/vazio.
    """
    if not lista_textos:
        return lista_textos

    if not texto_referencia:
        return lista_textos

    # Extrai tokens significativos do texto de referência
    tokens_ref = extrair_tokens_significativos(texto_referencia)

    # Se não há tokens significativos na referência, usa apenas comparação simples
    if not tokens_ref:
        # Filtro inline: remove duplicatas case-insensitive
        ref_lower = remove_accents(str(texto_referencia)).lower()
        resultado = []
        for texto in lista_textos:
            if not texto:
                continue
            texto_lower = remove_accents(str(texto)).lower()
            if texto_lower != ref_lower:
                resultado.append(texto)
        return resultado if resultado else None

    resultado = []

    for texto in lista_textos:
        if not texto:  # Skip None ou strings vazias
            continue

        # Extrai tokens do texto alternativo
        tokens_alt = extrair_tokens_significativos(texto)

        # Se não há tokens significativos, considera completamente diferente
        if not tokens_alt:
            continue

        # Calcula similaridade
        similaridade = calcular_jaccard_similarity(tokens_ref, tokens_alt)

        # Mantém apenas se está na faixa aceitável
        # threshold_minimo <= similaridade < threshold_maximo
        if threshold_minimo <= similaridade < threshold_maximo:
            resultado.append(texto)

    # Retorna None se lista ficou vazia
    return resultado if resultado else None
