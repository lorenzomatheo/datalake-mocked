from maggulake.utils.text_similarity import tokenize_for_matching


class TestTokenizeForMatching:
    def test_retorna_lista_vazia_quando_ambos_none(self):
        assert not tokenize_for_matching(None, None, 3, [], 10, 10)

    def test_processa_apenas_nome_quando_pa_none(self):
        tokens = tokenize_for_matching("DIPIRONA SÓDICA 500MG", None, 3, [], 10, 10)
        assert "DIPIRONA" in tokens
        assert "SODICA" in tokens

    def test_processa_apenas_pa_quando_nome_none(self):
        tokens = tokenize_for_matching(None, "PARACETAMOL + CAFEINA", 3, [], 10, 10)
        assert "PARACETAMOL" in tokens
        assert "CAFEINA" in tokens

    def test_combina_tokens_nome_e_pa(self):
        tokens = tokenize_for_matching(
            "TYLENOL DC 500MG", "PARACETAMOL + CODEINA", 3, [], 10, 10
        )
        assert "TYLENOL" in tokens
        assert "PARACETAMOL" in tokens
        assert "CODEINA" in tokens

    def test_remove_duplicatas_entre_nome_e_pa(self):
        tokens = tokenize_for_matching(
            "OMEPRAZOL MAGNÉSICO 20MG", "OMEPRAZOL", 3, [], 10, 10
        )
        assert "OMEPRAZOL" in tokens
        assert "MAGNESICO" in tokens
        assert tokens.count("OMEPRAZOL") == 1

    def test_respeita_tamanho_minimo(self):
        tokens = tokenize_for_matching("ABC DE FGH", None, 4, [], 10, 10)
        assert not tokens

        tokens = tokenize_for_matching("PRODUTO TESTE", None, 6, [], 10, 10)
        assert tokens == ["PRODUTO"]

    def test_aplica_blocklist(self):
        tokens = tokenize_for_matching(
            "OMEPRAZOL PRAZOL CONAZOL", None, 3, ["PRAZOL", "CONAZOL"], 10, 10
        )
        assert tokens == ["OMEPRAZOL"]

    def test_normaliza_acentos(self):
        tokens = tokenize_for_matching(
            "Ácido Acetilsalicílico", "Ácido Ascórbico", 3, [], 10, 10
        )
        assert "ACIDO" in tokens
        assert "ACETILSALICILICO" in tokens
        assert "ASCORBICO" in tokens

    def test_remove_numeros_e_unidades(self):
        tokens = tokenize_for_matching("DIPIRONA 500MG 30ML", None, 3, [], 10, 10)
        assert "DIPIRONA" in tokens

    def test_limite_max_termos_nome(self):
        tokens = tokenize_for_matching(
            "ALPHA BETA GAMMA DELTA EPSILON", None, 3, [], 3, 10
        )
        assert len(tokens) == 3
        assert "ALPHA" in tokens
        assert "BETA" in tokens
        assert "GAMMA" in tokens

    def test_limite_max_termos_pa(self):
        tokens = tokenize_for_matching(
            None, "OMEGA SIGMA THETA LAMBDA KAPPA", 3, [], 10, 2
        )
        assert len(tokens) == 2
        assert "OMEGA" in tokens
        assert "SIGMA" in tokens

    def test_strings_vazias_sao_ignoradas(self):
        assert not tokenize_for_matching("", "", 3, [], 10, 10)
        assert tokenize_for_matching("PRODUTO", "", 3, [], 10, 10) == ["PRODUTO"]
        assert tokenize_for_matching("", "PRINCIPIO", 3, [], 10, 10) == ["PRINCIPIO"]

    def test_caso_real_medicamento_completo(self):
        tokens = tokenize_for_matching(
            "LOSARTANA POTÁSSICA 50MG 30 COMPRIMIDOS",
            "LOSARTANA POTÁSSICA",
            5,
            [],
            10,
            10,
        )
        assert "LOSARTANA" in tokens
        assert "POTASSICA" in tokens
        assert "COMPRIMIDOS" in tokens

    def test_caso_real_nao_medicamento(self):
        tokens = tokenize_for_matching(
            "SHAMPOO HEAD & SHOULDERS ANTICASPA 400ML", None, 5, [], 10, 10
        )
        assert "SHAMPOO" in tokens
        assert "SHOULDERS" in tokens
        assert "ANTICASPA" in tokens
        assert "HEAD" not in tokens
