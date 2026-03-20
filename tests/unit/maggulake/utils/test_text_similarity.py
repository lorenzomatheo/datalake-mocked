import pytest

from maggulake.utils.text_similarity import (
    calcular_jaccard_similarity,
    extrair_tokens_significativos,
    filtrar_nomes_por_semelhanca,
)


class TestExtrairTokensSignificativos:
    """Testa extração de tokens significativos"""

    def test_retorna_set_vazio_quando_none(self):
        assert extrair_tokens_significativos(None) == set()

    def test_retorna_set_vazio_quando_vazio(self):
        assert extrair_tokens_significativos("") == set()

    def test_remove_numeros_isolados(self):
        tokens = extrair_tokens_significativos("Produto 100 200 300")
        assert tokens == {"produto"}

    def test_remove_unidades_de_medida(self):
        tokens = extrair_tokens_significativos("50 mg ml ui cp cps")
        assert tokens == set()

    def test_remove_numeros_com_unidades(self):
        tokens = extrair_tokens_significativos("DUZIMICIN 50MG 150ML")
        assert tokens == {"duzimicin"}

    def test_remove_palavras_curtas(self):
        # po, fr são < 3 caracteres
        tokens = extrair_tokens_significativos("PRODUTO PO FR AB")
        assert tokens == {"produto"}

    def test_mantem_palavras_significativas(self):
        tokens = extrair_tokens_significativos("Shampoo Pantene Cachos")
        assert tokens == {"shampoo", "pantene", "cachos"}

    def test_caso_real_medicamento(self):
        tokens = extrair_tokens_significativos("DUZIMICIN 50MG PO SUSP FR 150ML")
        assert tokens == {"duzimicin", "susp"}

    def test_caso_real_kit_vitamina(self):
        tokens = extrair_tokens_significativos("Vitamina D 2000UI Kit 30 Cápsulas")
        assert tokens == {"vitamina", "kit", "capsulas"}

    def test_remove_palavras_genericas(self):
        tokens = extrair_tokens_significativos("produto com para por sem")
        # 'com', 'para', 'por', 'sem' devem ser filtrados
        assert tokens == {"produto"}


class TestCalcularJaccardSimilarity:
    """Testa cálculo de Jaccard Similarity"""

    def test_retorna_zero_quando_primeira_set_vazia(self):
        assert calcular_jaccard_similarity(set(), {"a", "b"}) == 0.0

    def test_retorna_zero_quando_segunda_set_vazia(self):
        assert calcular_jaccard_similarity({"a", "b"}, set()) == 0.0

    def test_retorna_zero_quando_sem_elementos_em_comum(self):
        result = calcular_jaccard_similarity({"a"}, {"b"})
        assert result == 0.0

    def test_retorna_um_quando_identicos(self):
        result = calcular_jaccard_similarity({"a", "b"}, {"a", "b"})
        assert result == 1.0

    def test_calcula_similaridade_parcial(self):
        # 1 em comum (b), 3 no total (a, b, c)
        result = calcular_jaccard_similarity({"a", "b"}, {"b", "c"})
        assert result == pytest.approx(0.3333, abs=0.0001)

    def test_calcula_similaridade_metade(self):
        # 2 em comum, 4 no total
        result = calcular_jaccard_similarity(
            {"shampoo", "pantene", "cachos"}, {"pantene", "cachos", "hidravit"}
        )
        assert result == 0.5

    def test_ordem_nao_importa(self):
        result1 = calcular_jaccard_similarity({"a", "b"}, {"b", "c"})
        result2 = calcular_jaccard_similarity({"b", "c"}, {"a", "b"})
        assert result1 == result2


class TestFiltrarNomesPorSemelhanca:
    """Testa filtro por semelhança (teste de integração)"""

    def test_retorna_lista_vazia_quando_input_vazio(self):
        """Lista vazia no input retorna lista vazia, não None"""
        result = filtrar_nomes_por_semelhanca("texto", [])
        assert result == []

    def test_retorna_none_quando_lista_none(self):
        result = filtrar_nomes_por_semelhanca("texto", None)
        assert result is None

    def test_retorna_lista_original_quando_referencia_none(self):
        result = filtrar_nomes_por_semelhanca(None, ["A", "B"])
        assert result == ["A", "B"]

    def test_remove_nome_identico(self):
        """Similaridade ~1.0 >= 0.95 → remove"""
        result = filtrar_nomes_por_semelhanca(
            "Shampoo Pantene Cachos", ["SHAMPOO PANTENE CACHOS"]
        )
        # Idêntico (>= 0.95) → remove
        assert result is None

    def test_remove_nome_completamente_diferente(self):
        """Caso DUZIMICIN / PAROXETINA - similaridade 0.0 < 0.20"""
        result = filtrar_nomes_por_semelhanca(
            "DUZIMICIN 50MG PO SUSP FR 150ML",
            ["PAROXETINA CLORIDRATO (20MG); -  Conteudo: 60caps;"],
        )
        # Sem tokens em comum → similaridade 0.0 < 0.20 → remove
        assert result is None

    def test_mantem_nome_parcialmente_similar(self):
        """Similaridade entre 0.20 e 0.95"""
        result = filtrar_nomes_por_semelhanca(
            "Shampoo Pantene Cachos",
            ["PANTENE SH CACHOS HIDRAVIT"],  # 2/4 tokens = 0.5
        )
        assert result == ["PANTENE SH CACHOS HIDRAVIT"]

    def test_caso_problema_original(self):
        """Testa o caso exato do usuário"""
        result = filtrar_nomes_por_semelhanca(
            "DUZIMICIN 50MG PO SUSP FR 150ML",
            [
                "PAROXETINA CLORIDRATO (20MG); -  Conteudo: 60caps;",
                "DUZIMICIN 50MG PO SUSP FR 150ML,",
            ],
        )
        # Primeiro: sem semelhança → remove
        # Segundo: idêntico → remove
        assert result is None

    def test_custom_thresholds_minimo(self):
        """Testa com threshold mínimo mais permissivo"""
        result = filtrar_nomes_por_semelhanca(
            "Produto ABC", ["PRODUTO XYZ"], threshold_minimo=0.10, threshold_maximo=0.95
        )
        # tokens: {'produto', 'abc'} vs {'produto', 'xyz'}
        # similaridade: 1/3 = 0.33 >= 0.10 → mantém
        assert result == ["PRODUTO XYZ"]

    def test_custom_thresholds_maximo(self):
        """Testa com threshold máximo mais rigoroso"""
        result = filtrar_nomes_por_semelhanca(
            "Produto ABC",
            ["PRODUTO ABC XYZ"],
            threshold_minimo=0.20,
            threshold_maximo=0.99,
        )
        # tokens: {'produto', 'abc'} vs {'produto', 'abc', 'xyz'}
        # similaridade: 2/3 = 0.66 < 0.99 → mantém
        assert result == ["PRODUTO ABC XYZ"]

    def test_skip_none_em_lista(self):
        result = filtrar_nomes_por_semelhanca(
            "Shampoo Pantene", ["Shampoo Dove", None, "", "Condicionador Pantene"]
        )
        # None e "" são skipped
        assert len(result) >= 1  # Pelo menos um deve ser mantido

    def test_fallback_para_filtro_identicas_quando_sem_tokens_significativos(self):
        """Se não há tokens significativos, usa filtro básico"""
        result = filtrar_nomes_por_semelhanca(
            "123 456",  # Só números → sem tokens
            ["123 456", "789"],
        )
        # "123 456" é idêntico → remove
        # "789" é diferente → mantém
        assert result == ["789"]

    def test_mantem_variacao_com_info_adicional(self):
        """Variações com informação adicional devem ser mantidas"""
        result = filtrar_nomes_por_semelhanca(
            "Shampoo Pantene", ["SHAMPOO PANTENE CACHOS HIDRAVITAMINADOS"]
        )
        # tokens: {'shampoo', 'pantene'} vs {'shampoo', 'pantene', 'cachos', 'hidravitaminados'}
        # similaridade: 2/4 = 0.5 → entre 0.20 e 0.95 → mantém
        assert result == ["SHAMPOO PANTENE CACHOS HIDRAVITAMINADOS"]
