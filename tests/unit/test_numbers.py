from maggulake.utils.numbers import assign_quintile, calculate_mode, map_faixa_etaria


class TestCalculateMode:
    def test_single_mode(self):
        assert calculate_mode([1, 2, 2, 3, 4]) == 2

    def test_multiple_modes(self):
        assert calculate_mode([1, 1, 2, 2, 3]) in [1, 2]

    def test_no_mode(self):
        assert calculate_mode([]) is None

    def test_all_unique(self):
        assert calculate_mode([1, 2, 3, 4, 5]) in [1, 2, 3, 4, 5]

    def test_single_element(self):
        assert calculate_mode([5]) == 5


class TestAssignQuintile:
    def test_assign_quintile_ascending(self):
        assert assign_quintile(1, [1, 2, 3, 4]) == 1
        assert assign_quintile(2, [1, 2, 3, 4]) == 2
        assert assign_quintile(25.2, [1, 10, 30, 35]) == 3
        assert assign_quintile(50, [1, 10, 30, 35]) == 5

    def test_assign_quintile_descending(self):
        assert assign_quintile(1, [1, 2, 3, 4], ascending=False) == 5
        assert assign_quintile(2, [1, 2, 3, 4], ascending=False) == 4
        assert assign_quintile(25.2, [1, 10, 30, 35], ascending=False) == 3
        assert assign_quintile(50, [1, 10, 30, 35], ascending=False) == 1

    def test_assign_quintile_edge_cases(self):
        assert assign_quintile(0, [1, 2, 3, 4]) == 1
        assert assign_quintile(5, [1, 2, 3, 4]) == 5
        assert assign_quintile(0, [1, 2, 3, 4], ascending=False) == 5
        assert assign_quintile(5, [1, 2, 3, 4], ascending=False) == 1


class TestMapFaixaEtaria:
    def test_bebe(self):
        """Testa faixa etária bebê (0-2 anos)"""
        assert map_faixa_etaria(0) == "bebe"
        assert map_faixa_etaria(1) == "bebe"
        assert map_faixa_etaria(0.5) == "bebe"

    def test_crianca(self):
        """Testa faixa etária criança (3-12 anos)"""
        assert map_faixa_etaria(3) == "crianca"
        assert map_faixa_etaria(7) == "crianca"
        assert map_faixa_etaria(11) == "crianca"
        assert map_faixa_etaria(5.5) == "crianca"

    def test_adolescente(self):
        """Testa faixa etária adolescente (13-17 anos)"""
        assert map_faixa_etaria(13) == "adolescente"
        assert map_faixa_etaria(15) == "adolescente"
        assert map_faixa_etaria(17) == "adolescente"

    def test_jovem(self):
        """Testa faixa etária jovem (18-24 anos)"""
        assert map_faixa_etaria(19) == "jovem"
        assert map_faixa_etaria(21) == "jovem"
        assert map_faixa_etaria(23) == "jovem"

    def test_adulto(self):
        """Testa faixa etária adulto (25-59 anos)"""
        assert map_faixa_etaria(25) == "adulto"
        assert map_faixa_etaria(40) == "adulto"
        assert map_faixa_etaria(59) == "adulto"

    def test_idoso(self):
        """Testa faixa etária idoso (60+ anos)"""
        assert map_faixa_etaria(60) == "idoso"
        assert map_faixa_etaria(75) == "idoso"
        assert map_faixa_etaria(100) == "idoso"

    def test_none_input(self):
        """Testa entrada None"""
        assert map_faixa_etaria(None) is None

    def test_negative_age(self):
        """Testa idade negativa"""
        assert map_faixa_etaria(-1) is None
        assert map_faixa_etaria(-10) is None

    def test_float_values(self):
        """Testa valores decimais"""
        assert map_faixa_etaria(1.9) == "bebe"
        assert map_faixa_etaria(11.5) == "crianca"
        assert map_faixa_etaria(17.9) == "adolescente"
        assert map_faixa_etaria(22.1) == "jovem"
        assert map_faixa_etaria(30.5) == "adulto"
