"""Testes unitários para maggulake.ativacao.whatsapp.hyperflow.utils"""

import pytest

from maggulake.ativacao.whatsapp.exceptions import NumeroTelefoneInvalidoError
from maggulake.ativacao.whatsapp.hyperflow.utils import padroniza_numero_whatsapp


class TestPadronizaNumeroWhatsappValido:
    """Testes para entradas válidas na função padroniza_numero_whatsapp"""

    def test_numero_brasileiro_com_ddd_e_nono_digito(self):
        resultado = padroniza_numero_whatsapp("11987654321")
        assert resultado == "11987654321"

    def test_numero_brasileiro_com_codigo_pais_55(self):
        resultado = padroniza_numero_whatsapp("5511987654321")
        assert resultado == "11987654321"

    def test_numero_com_espacos_e_tracos(self):
        resultado = padroniza_numero_whatsapp("11 9 8765-4321")
        assert resultado == "11987654321"

    def test_numero_com_parenteses_no_ddd(self):
        resultado = padroniza_numero_whatsapp("(11) 98765-4321")
        assert resultado == "11987654321"

    def test_numero_formatado_completo(self):
        resultado = padroniza_numero_whatsapp("+55 (11) 9 8765-4321")
        assert resultado == "11987654321"

    def test_numero_com_espacos_no_inicio_e_fim(self):
        resultado = padroniza_numero_whatsapp("  11987654321  ")
        assert resultado == "11987654321"

    def test_numero_com_multiplos_espacos_e_tracos(self):
        resultado = padroniza_numero_whatsapp("11  --  987  --  654  --  321")
        assert resultado == "11987654321"

    def test_numero_exatamente_11_digitos_sem_55(self):
        resultado = padroniza_numero_whatsapp("11987654321")
        assert resultado == "11987654321"

    def test_numero_menor_que_11_digitos(self):
        resultado = padroniza_numero_whatsapp("1198765432")
        assert resultado == "1198765432"

    def test_numero_celular_antigo_sem_nono_digito(self):
        resultado = padroniza_numero_whatsapp("1187654321")
        assert resultado == "1187654321"

    def test_numero_fixo_brasileiro(self):
        resultado = padroniza_numero_whatsapp("1133334444")
        assert resultado == "1133334444"


class TestPadronizaNumeroWhatsappInvalido:
    """Testes para entradas inválidas na função padroniza_numero_whatsapp"""

    def test_numero_menor_que_10_digitos(self):
        with pytest.raises(
            NumeroTelefoneInvalidoError,
            match="Número inválido: deve conter pelo menos 10 dígitos",
        ):
            padroniza_numero_whatsapp("119876543")

    def test_numero_vazio(self):
        with pytest.raises(
            NumeroTelefoneInvalidoError,
            match="Número inválido: deve conter pelo menos 10 dígitos",
        ):
            padroniza_numero_whatsapp("")

    def test_numero_apenas_espacos_e_caracteres_especiais(self):
        with pytest.raises(
            NumeroTelefoneInvalidoError,
            match="Número inválido: deve conter pelo menos 10 dígitos",
        ):
            padroniza_numero_whatsapp("  - ( ) -  ")

    def test_tipo_invalido_none(self):
        with pytest.raises(TypeError, match="Número deve ser uma string."):
            padroniza_numero_whatsapp(None)

    def test_tipo_invalido_int(self):
        with pytest.raises(TypeError, match="Número deve ser uma string."):
            padroniza_numero_whatsapp(11987654321)

    def test_tipo_invalido_float(self):
        with pytest.raises(TypeError, match="Número deve ser uma string."):
            padroniza_numero_whatsapp(11.987654321)

    def test_tipo_invalido_list(self):
        with pytest.raises(TypeError, match="Número deve ser uma string."):
            padroniza_numero_whatsapp(["11987654321"])

    def test_numero_com_todos_zeros(self):
        with pytest.raises(
            NumeroTelefoneInvalidoError,
            match="Número inválido: não pode ser uma sequência do mesmo dígito.",
        ):
            padroniza_numero_whatsapp("00000000000")

    def test_numero_com_todos_cincos(self):
        with pytest.raises(
            NumeroTelefoneInvalidoError,
            match="Número inválido: não pode ser uma sequência do mesmo dígito.",
        ):
            padroniza_numero_whatsapp("55555555555")

    def test_numero_com_todos_noves(self):
        with pytest.raises(
            NumeroTelefoneInvalidoError,
            match="Número inválido: não pode ser uma sequência do mesmo dígito.",
        ):
            padroniza_numero_whatsapp("99999999999")
