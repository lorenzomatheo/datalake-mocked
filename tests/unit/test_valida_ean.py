from maggulake.produtos.valida_ean import valida_ean


class TestValidaEan:
    def test_valida_ean(self):
        assert valida_ean(None) == False
        assert valida_ean(1234) == False
        assert valida_ean("1234") == False
        assert valida_ean("7896206403638") == True
        assert valida_ean("12348") == True
