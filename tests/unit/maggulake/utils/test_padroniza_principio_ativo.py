import pytest

from maggulake.produtos.padroniza_principio_ativo import padronizar_principio_ativo


class TestPadronizarPrincipioAtivoBasico:
    def test_none_returns_none(self):
        assert padronizar_principio_ativo(None) is None

    def test_empty_string_returns_empty(self):
        assert padronizar_principio_ativo("") == ""

    def test_whitespace_only_returns_whitespace(self):
        assert padronizar_principio_ativo("   ") == "   "

    def test_simple_value_uppercased(self):
        assert padronizar_principio_ativo("paracetamol") == "PARACETAMOL"

    def test_removes_accents(self):
        assert (
            padronizar_principio_ativo("ácido acetilsalicílico")
            == "ACIDO ACETILSALICILICO"
        )

    def test_semicolon_separator(self):
        result = padronizar_principio_ativo("paracetamol;cafeina")
        assert result == "CAFEINA + PARACETAMOL"

    def test_comma_separator_before_letter(self):
        result = padronizar_principio_ativo("paracetamol, cafeina")
        assert result == "CAFEINA + PARACETAMOL"

    def test_newline_separator(self):
        result = padronizar_principio_ativo("paracetamol\ncafeina")
        assert result == "CAFEINA + PARACETAMOL"

    def test_plus_separator_preserved(self):
        result = padronizar_principio_ativo("paracetamol + cafeina")
        assert result == "CAFEINA + PARACETAMOL"

    def test_alphabetical_ordering(self):
        result = padronizar_principio_ativo("zolpidem + alprazolam + diazepam")
        assert result == "ALPRAZOLAM + DIAZEPAM + ZOLPIDEM"

    def test_deduplication(self):
        result = padronizar_principio_ativo("paracetamol + paracetamol")
        assert result == "PARACETAMOL"

    def test_crlf_handling(self):
        result = padronizar_principio_ativo("paracetamol\r\ncafeina")
        assert result == "CAFEINA + PARACETAMOL"

    def test_multiple_spaces_collapsed(self):
        result = padronizar_principio_ativo("paracetamol   +   cafeina")
        assert result == "CAFEINA + PARACETAMOL"

    def test_complex_multi_component(self):
        result = padronizar_principio_ativo("losartana potássica;hidroclorotiazida")
        assert "HIDROCLOROTIAZIDA" in result
        assert "LOSARTANA POTASSICA" in result


class TestPadronizarPrincipioAtivoSalDosagemAcido:
    def test_salt_form_parenthesis(self):
        result = padronizar_principio_ativo("metformina (cloridrato)")
        assert result == "CLORIDRATO DE METFORMINA"

    def test_salt_form_postfix(self):
        result = padronizar_principio_ativo("metformina cloridrato")
        assert result == "CLORIDRATO DE METFORMINA"

    def test_salt_form_prefix_without_de(self):
        result = padronizar_principio_ativo("cloridrato metformina")
        assert result == "CLORIDRATO DE METFORMINA"

    def test_salt_form_with_de_unchanged(self):
        result = padronizar_principio_ativo("cloridrato de metformina")
        assert result == "CLORIDRATO DE METFORMINA"

    def test_hydration_triidratado(self):
        result = padronizar_principio_ativo("amoxicilina triidratado")
        assert "TRI-HIDRATADO" in result

    def test_hydration_trihidratado(self):
        result = padronizar_principio_ativo("amoxicilina trihidratado")
        assert "TRI-HIDRATADO" in result

    def test_hydration_monohidratado(self):
        result = padronizar_principio_ativo("cefalexina monohidratado")
        assert "MONOIDRATADO" in result

    def test_acid_inversion(self):
        result = padronizar_principio_ativo("acetilsalicilico acido")
        assert result == "ACIDO ACETILSALICILICO"

    def test_acid_already_first(self):
        result = padronizar_principio_ativo("acido acetilsalicilico")
        assert result == "ACIDO ACETILSALICILICO"

    def test_removes_mg_dosage_in_parenthesis(self):
        result = padronizar_principio_ativo("paracetamol (500 mg)")
        assert result == "PARACETAMOL"

    def test_removes_percentage_dosage(self):
        result = padronizar_principio_ativo("clorexidina 0,12%")
        assert result == "CLOREXIDINA"

    def test_removes_mg_dosage_suffix(self):
        result = padronizar_principio_ativo("losartana 50mg")
        assert result == "LOSARTANA"

    def test_removes_mg_ml_dosage(self):
        result = padronizar_principio_ativo("dipirona 500mg/ml")
        assert result == "DIPIRONA"

    def test_removes_non_salt_parentheses(self):
        result = padronizar_principio_ativo("paracetamol (comp rosado)")
        assert result == "PARACETAMOL"

    @pytest.mark.parametrize(
        "sal",
        [
            "acetato",
            "cloridrato",
            "sulfato",
            "fosfato",
            "maleato",
            "fumarato",
            "tartarato",
            "mesilato",
            "nitrato",
            "citrato",
        ],
    )
    def test_all_common_salt_forms(self, sal: str):
        result = padronizar_principio_ativo(f"substancia ({sal})")
        expected = f"{sal.upper()} DE SUBSTANCIA"
        assert result == expected
