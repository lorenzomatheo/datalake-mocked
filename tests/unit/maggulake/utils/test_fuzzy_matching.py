from maggulake.utils.fuzzy_matching import fuzzy_matching, tokenize


class TestTokenize:
    def test_tokenize_normalizes_and_deduplicates_tokens(self):
        tokens = tokenize("Café com Açúcar, café!")

        assert tokens == {"cafe", "com", "acucar"}


class TestFuzzyMatching:
    def test_returns_none_when_input_is_empty(self):
        assert fuzzy_matching("", ["anything"]) is None
        assert fuzzy_matching(None, ["anything"]) is None

    def test_uses_real_scores_when_available(self):
        options = ["Alpha", "Alpha Beta Classic", "Completely different"]

        result = fuzzy_matching("Alpha beta", options)

        assert result == "Alpha Beta Classic"

    def test_handles_typos_and_accents(self):
        options = ["Café da Manhã", "Farmácia Popular", "Estoque Beta"]

        result = fuzzy_matching("Cafe da manha", options)

        assert result == "Café da Manhã"

    def test_prefers_higher_token_overlap_over_score(self, monkeypatch):
        mapping = {
            "ratio": ("Alpha Beta Classic", 80, None),
            "partial_ratio": ("Alpha", 95, None),
            "token_sort_ratio": ("Alpha Beta Classic", 82, None),
            "token_set_ratio": ("Alpha Beta Classic", 83, None),
            "WRatio": ("Alpha", 97, None),
            "QRatio": ("Alpha Beta Classic", 84, None),
        }

        def fake_extract_one(query, choices, scorer):  # pylint: disable=unused-argument
            return mapping.get(scorer.__name__, ("Alpha", 50, None))

        monkeypatch.setattr(
            "maggulake.utils.fuzzy_matching.process.extractOne", fake_extract_one
        )

        result = fuzzy_matching(
            "Alpha beta", ["Alpha", "Alpha Beta Classic", "Completely different"]
        )

        assert result == "Alpha Beta Classic"

    def test_returns_none_when_no_token_overlap(self, monkeypatch):
        def fake_extract_one(query, choices, scorer):  # pylint: disable=unused-argument
            return ("Gamma Delta", 60, None)

        monkeypatch.setattr(
            "maggulake.utils.fuzzy_matching.process.extractOne", fake_extract_one
        )

        result = fuzzy_matching("Alpha Beta", ["Gamma Delta", "Theta Kappa"])

        assert result is None

    def test_returns_none_when_no_token_overlap_with_real_scorer(self):
        result = fuzzy_matching("Alpha Beta", ["Gamma Delta", "Theta Kappa"])

        assert result is None
