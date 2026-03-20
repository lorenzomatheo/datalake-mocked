import numpy as np
import pytest

from maggulake.utils.strings import (
    concat_categories,
    converte_array_para_string,
    limpa_cnpj,
    normalize_text_alphanumeric,
    remove_accents,
    remove_special_characters,
    sanitize_string,
    standardize_phone_number,
    string_to_column_name,
    validate_email_return_none_if_fail,
)


class TestRemoveAccents:
    def test_remove_accents(self):
        assert remove_accents("João") == "Joao"
        assert remove_accents("João José") == "Joao Jose"
        assert remove_accents("João José Caiçara") == "Joao Jose Caicara"
        assert (
            remove_accents("Canadá em Português é Canadá")
            == "Canada em Portugues e Canada"
        )
        assert remove_accents("Canaã dos Carajás") == "Canaa dos Carajas"
        assert remove_accents(None) == ""


class TestConverteArrayParaString:
    def test_converte_array_para_string(self):
        assert converte_array_para_string(['a', 'b', 'c']) == 'a|b|c'
        assert converte_array_para_string([]) is None
        assert converte_array_para_string(("a", "b", "c", "d")) == 'a|b|c|d'


class TestSanitizeString:
    def test_sanitize_string(self):
        assert sanitize_string("João") == 'joao'
        assert sanitize_string("Água com gás.") == 'agua_com_gas'
        assert (
            sanitize_string("Tem água c/ gás para o José beber no Canadá amanhã?")
            == 'tem_agua_c_gas_para_o_jose_beber_no_canada_amanha'
        )
        assert sanitize_string("Eu não sei! Você sabe?") == 'eu_nao_sei_voce_sabe'


class TestSanitizePhoneNumber:
    def test_standardize_phone_number(self):
        assert standardize_phone_number("99907-1189") == '99907-1189'
        assert standardize_phone_number("985867555") == '98586-7555'
        assert standardize_phone_number("(13)91479-7447") == '(13) 91479-7447'
        assert standardize_phone_number("(11) 95991-7000") == '(11) 95991-7000'
        assert standardize_phone_number("98259-2200") == '98259-2200'
        assert standardize_phone_number("1197722-1205") == '(11) 97722-1205'
        assert standardize_phone_number("011985867555") == '(11) 98586-7555'
        assert standardize_phone_number("011999071189") == '(11) 99907-1189'
        assert standardize_phone_number("11999999999") == '(11) 99999-9999'
        assert standardize_phone_number("999999999") == '99999-9999'
        assert standardize_phone_number("12345678") == '1234-5678'


class TestConcatCategories:
    def test_concat_categories(self):
        assert concat_categories(['a', 'b', 'c']) == 'a|b|c'
        assert concat_categories(['a', None, 'c']) == 'a|c'
        assert concat_categories([]) == ''
        assert concat_categories(np.array(['a', 'b', 'c'])) == 'a|b|c'
        assert concat_categories(np.array(['a', None, 'c'])) == 'a|c'
        assert concat_categories(None) == ''


class TestRemoveSpecialCharacters:
    def test_remove_special_characters(self):
        assert remove_special_characters("Hello, World!") == "Hello World"
        assert remove_special_characters("123-456-7890") == "123-456-7890"
        assert remove_special_characters("Café com leite!") == "Caf com leite"
        assert remove_special_characters("Água com gás.") == "gua com gs."
        assert remove_special_characters("!@#$%^&*()") == ""
        assert remove_special_characters(12345) == "12345"


class TestLimpaCnpj:
    def test_limpa_cnpj(self):
        assert limpa_cnpj("12.345.678/0001-95") == "12345678000195"
        assert limpa_cnpj("00.000.000/0000-00") == ""
        assert limpa_cnpj("12.345.678/0001-95\n") == "12345678000195"
        assert limpa_cnpj(" 12.345.678/0001-95 ") == "12345678000195"
        assert limpa_cnpj("12345678000195") == "12345678000195"
        assert limpa_cnpj("0012345678000195") == "12345678000195"
        assert limpa_cnpj(12345678000195) == "12345678000195"
        assert limpa_cnpj("") == ""
        assert limpa_cnpj(None) == "None"


class TestValidateEmailReturnNoneIfFail:
    def test_returns_none_for_invalid_emails(self):
        assert validate_email_return_none_if_fail(None) is None
        assert validate_email_return_none_if_fail('') is None
        assert validate_email_return_none_if_fail('   ') is None
        assert validate_email_return_none_if_fail('testexample.com') is None
        assert validate_email_return_none_if_fail('test@examplecom') is None
        assert validate_email_return_none_if_fail('algo.email@algo') is None

    def test_returns_email_for_valid_emails(self):
        assert (
            validate_email_return_none_if_fail('test@example.com') == 'test@example.com'
        )
        assert (
            validate_email_return_none_if_fail('  test@example.com  ')
            == 'test@example.com'
        )
        assert (
            validate_email_return_none_if_fail('test.name@example.co.uk')
            == 'test.name@example.co.uk'
        )


class TestNormalizeTextAlphanumeric:
    """Testa normalização de texto para comparação alfanumérica"""

    def test_retorna_none_quando_input_none(self):
        assert normalize_text_alphanumeric(None) is None

    def test_remove_acentos(self):
        assert normalize_text_alphanumeric("água") == "agua"
        assert normalize_text_alphanumeric("café") == "cafe"
        assert normalize_text_alphanumeric("Atenção") == "atencao"

    def test_converte_para_lowercase(self):
        assert normalize_text_alphanumeric("PRODUTO") == "produto"
        assert normalize_text_alphanumeric("PrOdUtO") == "produto"

    def test_remove_espacos(self):
        assert normalize_text_alphanumeric("produto teste") == "produtoteste"
        assert (
            normalize_text_alphanumeric("  espaços  múltiplos  ") == "espacosmultiplos"
        )

    def test_remove_pontuacao(self):
        assert normalize_text_alphanumeric("produto-teste") == "produtoteste"
        assert normalize_text_alphanumeric("produto.teste") == "produtoteste"
        assert normalize_text_alphanumeric("produto,teste") == "produtoteste"

    def test_remove_caracteres_especiais(self):
        assert normalize_text_alphanumeric("produto®") == "produto"
        assert normalize_text_alphanumeric("produto™") == "produto"
        assert normalize_text_alphanumeric("produto@#$%") == "produto"

    def test_mantem_numeros(self):
        assert normalize_text_alphanumeric("produto123") == "produto123"
        assert normalize_text_alphanumeric("500mg") == "500mg"

    def test_caso_real_aspirina(self):
        assert normalize_text_alphanumeric("Aspirina® 500mg") == "aspirina500mg"

    def test_caso_real_agua_sanitaria(self):
        assert normalize_text_alphanumeric("Água Sanitária - 1L") == "aguasanitaria1l"

    def test_caso_real_medicamento_complexo(self):
        assert (
            normalize_text_alphanumeric("Dorflex® Comprimidos 36cp")
            == "dorflexcomprimidos36cp"
        )

    def test_marcas_diferentes_mesmo_nome(self):
        # Usado no notebook para comparar marcas e fabricantes
        marca1 = normalize_text_alphanumeric("Novartis Pharma")
        marca2 = normalize_text_alphanumeric("NOVARTIS PHARMA")
        marca3 = normalize_text_alphanumeric("Novartis  Pharma  ")
        assert marca1 == marca2 == marca3 == "novartispharma"

    def test_fabricantes_com_pontuacao(self):
        fab1 = normalize_text_alphanumeric("Johnson & Johnson")
        fab2 = normalize_text_alphanumeric("Johnson&Johnson")
        fab3 = normalize_text_alphanumeric("JOHNSON & JOHNSON")
        assert fab1 == fab2 == fab3 == "johnsonjohnson"


class TestStringToColumnName:
    @pytest.mark.parametrize(
        "input_str, expected",
        [
            # básicos
            ("Nome", "nome"),
            ("  Nome  ", "nome"),
            ("DATA DE NASCIMENTO", "data_de_nascimento"),
            # substituições de espaços/pontuação
            ("coluna,com;virgula", "coluna_com_virgula"),
            ("coluna=com=igual", "coluna_com_igual"),
            ("coluna\tcom\ttab", "coluna_com_tab"),
            ("coluna\ncom\nnova", "coluna_com_nova"),
            # remove { } ( )
            ("coluna{com}parenteses", "colunacomparenteses"),
            ("(coluna)", "coluna"),
            ("{coluna}", "coluna"),
            # acentos
            ("São Paulo", "sao_paulo"),
            (" José da Silva ", "jose_da_silva"),
            ("Coração", "coracao"),
            # string vazia
            ("", ""),
            # só caracteres especiais
            ("{}(),;", "__"),
            ("   \t\n  ", ""),
            # misto geral
            ("  (Receita Líquida) = Valor Final; ", "receita_liquida___valor_final_"),
        ],
    )
    def test_string_to_column_name(self, input_str, expected):
        assert string_to_column_name(input_str) == expected
