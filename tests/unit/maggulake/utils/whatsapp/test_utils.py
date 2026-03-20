import pandas as pd
import pytest
import pytz

from maggulake.ativacao.whatsapp.utils import (
    extrair_vocativo,
    gerar_saudacao_com_base_na_hora,
)


class TestGerarSaudacao:
    tz = pytz.timezone("America/Sao_Paulo")

    def test_gerar_saudacao_bom_dia(self, mocker):
        mock_now = pd.Timestamp("2023-01-01 10:00:00", tz=self.tz)
        mocker.patch("pandas.Timestamp.now", return_value=mock_now)
        assert gerar_saudacao_com_base_na_hora(tz=self.tz) == "Bom dia"

    def test_gerar_saudacao_boa_tarde(self, mocker):
        mock_now = pd.Timestamp("2023-01-01 14:00:00", tz=self.tz)
        mocker.patch("pandas.Timestamp.now", return_value=mock_now)
        assert gerar_saudacao_com_base_na_hora(tz=self.tz) == "Boa tarde"

    def test_gerar_saudacao_boa_noite(self, mocker):
        """Testa se a saudação é 'Boa noite' à noite."""
        mock_now = pd.Timestamp("2023-01-01 23:00:00", tz=self.tz)
        mocker.patch("pandas.Timestamp.now", return_value=mock_now)
        assert gerar_saudacao_com_base_na_hora(tz=self.tz) == "Boa noite"

    def test_gerar_saudacao_meio_dia(self, mocker):
        mock_now = pd.Timestamp("2023-01-01 12:00:00", tz=self.tz)
        mocker.patch("pandas.Timestamp.now", return_value=mock_now)
        assert gerar_saudacao_com_base_na_hora(tz=self.tz) == "Boa tarde"

    def test_gerar_saudacao_seis_da_tarde(self, mocker):
        mock_now = pd.Timestamp("2023-01-01 18:00:00", tz=self.tz)
        mocker.patch("pandas.Timestamp.now", return_value=mock_now)
        assert gerar_saudacao_com_base_na_hora(tz=self.tz) == "Boa noite"


class TestExtrairVocativo:
    @pytest.mark.parametrize(
        "nome, esperado",
        [
            ("João da Silva", "João"),
            ("Maria", "Maria"),
            (" Dr. Carlos ", "Dr."),
            ("  Pedro  ", "Pedro"),
            ("Ana Clara", "Ana"),
        ],
    )
    def test_extrair_vocativo_nomes_validos(self, nome, esperado):
        """Testa a extração do vocativo de nomes válidos."""
        assert extrair_vocativo(nome) == esperado

    @pytest.mark.parametrize(
        "nome",
        [
            None,
            pd.NA,
            "",
            "   ",
        ],
    )
    def test_extrair_vocativo_nomes_invalidos(self, nome):
        assert extrair_vocativo(nome) is None
