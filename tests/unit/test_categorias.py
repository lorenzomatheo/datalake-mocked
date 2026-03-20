"""Testes unitários para a classe CategoriasWrapper."""

import pytest

from maggulake.enums.categorias import CategoriasWrapper, arvore_categorias


class TestGetArvoreCategoria:
    """Testes para o método get_arvore_categorias."""

    def test_retorna_dict_com_estrutura_correta(self):
        wrapper = CategoriasWrapper()
        arvore = wrapper.get_arvore_categorias()

        assert isinstance(arvore, dict)
        assert len(arvore) > 0
        # Verifica que cada super categoria tem meso categorias
        for super_cat, meso_dict in arvore.items():
            assert isinstance(super_cat, str)
            assert isinstance(meso_dict, dict)
            # Verifica que cada meso categoria tem micro categorias (lista)
            for meso_cat, micro_list in meso_dict.items():
                assert isinstance(meso_cat, str)
                assert isinstance(micro_list, list)

    def test_contem_categorias_conhecidas(self):
        wrapper = CategoriasWrapper()
        arvore = wrapper.get_arvore_categorias()

        # Verifica algumas categorias conhecidas
        assert "Medicamentos" in arvore
        assert "Perfumaria e Cuidados" in arvore
        assert "Sistema Nervoso" in arvore["Medicamentos"]
        assert "Ansiedade" in arvore["Medicamentos"]["Sistema Nervoso"]


class TestGetAllSuperCategorias:
    """Testes para o método get_all_super_categorias."""

    def test_retorna_lista_de_strings(self):
        wrapper = CategoriasWrapper()
        super_cats = wrapper.get_all_super_categorias()

        assert isinstance(super_cats, list)
        assert len(super_cats) > 0
        assert all(isinstance(cat, str) for cat in super_cats)

    def test_contem_categorias_esperadas(self):
        wrapper = CategoriasWrapper()
        super_cats = wrapper.get_all_super_categorias()

        assert "Medicamentos" in super_cats
        assert "Perfumaria e Cuidados" in super_cats
        assert "Produtos para Casa" in super_cats
        assert "Materiais para Saúde" in super_cats
        assert "Alimentos e Suplementos" in super_cats
        assert "Produtos para Animais" in super_cats


class TestGetAllMesoCategorias:
    """Testes para o método get_all_meso_categorias."""

    def test_retorna_meso_categorias_de_medicamentos(self):
        wrapper = CategoriasWrapper()
        meso_cats = wrapper.get_all_meso_categorias("Medicamentos")

        assert isinstance(meso_cats, list)
        assert len(meso_cats) > 0
        assert "Sistema Nervoso" in meso_cats
        assert "Dor e Febre" in meso_cats
        assert "Sistema Circulatório" in meso_cats

    def test_retorna_lista_vazia_para_super_inexistente(self):
        wrapper = CategoriasWrapper()
        meso_cats = wrapper.get_all_meso_categorias("Categoria Inexistente")

        assert not meso_cats


class TestGetAllMicroCategorias:
    """Testes para o método get_all_micro_categorias."""

    def test_retorna_micro_categorias_corretas(self):
        wrapper = CategoriasWrapper()
        micro_cats = wrapper.get_all_micro_categorias("Medicamentos", "Sistema Nervoso")

        assert isinstance(micro_cats, list)
        assert len(micro_cats) > 0
        assert "Ansiedade" in micro_cats
        assert "Insônia" in micro_cats
        assert "Antidepressivos" in micro_cats

    def test_retorna_lista_vazia_para_combinacao_inexistente(self):
        wrapper = CategoriasWrapper()
        micro_cats = wrapper.get_all_micro_categorias(
            "Medicamentos", "Categoria Inexistente"
        )

        assert not micro_cats


class TestGetAllCategoriasFlat:
    """Testes para o método get_all_categorias_flat."""

    def test_retorna_lista_de_strings_formatadas(self):
        wrapper = CategoriasWrapper()
        cats_flat = wrapper.get_all_categorias_flat()

        assert isinstance(cats_flat, list)
        assert len(cats_flat) > 0
        assert all(isinstance(cat, str) for cat in cats_flat)
        # Verifica formato: "Super -> Meso -> Micro"
        assert any(" -> " in cat for cat in cats_flat)

    def test_contem_categorias_conhecidas_flat(self):
        wrapper = CategoriasWrapper()
        cats_flat = wrapper.get_all_categorias_flat()

        assert "Medicamentos -> Sistema Nervoso -> Ansiedade" in cats_flat
        assert "Perfumaria e Cuidados -> Cuidado com a Pele -> Hidratantes" in cats_flat


class TestGetSuperAndMesoFromMicro:
    """Testes para o método get_super_and_meso_from_micro."""

    def test_encontra_categoria_unica(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_super_and_meso_from_micro("Ansiedade")

        assert isinstance(resultado, list)
        assert len(resultado) >= 1
        assert ("Medicamentos", "Sistema Nervoso", "Ansiedade") in resultado

    def test_retorna_lista_vazia_para_micro_inexistente(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_super_and_meso_from_micro("Categoria Inexistente")

        assert not resultado


class TestGetSuperFromMeso:
    """Testes para o método get_super_from_meso."""

    def test_encontra_super_categoria(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_super_from_meso("Sistema Nervoso")

        assert isinstance(resultado, list)
        assert len(resultado) >= 1
        assert "Medicamentos" in resultado

    def test_retorna_lista_vazia_para_meso_inexistente(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_super_from_meso("Categoria Inexistente")

        assert not resultado


class TestGetMicrosFromMeso:
    """Testes para o método get_micros_from_meso."""

    def test_retorna_micros_com_super_especificada(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_micros_from_meso(
            "Sistema Nervoso", super_categoria="Medicamentos"
        )

        assert isinstance(resultado, list)
        assert len(resultado) > 0
        assert "Ansiedade" in resultado

    def test_retorna_micros_sem_super_especificada(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_micros_from_meso("Sistema Nervoso")

        assert isinstance(resultado, list)
        assert len(resultado) > 0
        assert "Ansiedade" in resultado

    def test_retorna_lista_vazia_para_meso_inexistente(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_micros_from_meso("Categoria Inexistente")

        assert not resultado


class TestGetMesoAndMicrosFromSuper:
    """Testes para o método get_meso_and_micros_from_super."""

    def test_retorna_dict_correto(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_meso_and_micros_from_super("Medicamentos")

        assert isinstance(resultado, dict)
        assert len(resultado) > 0
        assert "Sistema Nervoso" in resultado
        assert isinstance(resultado["Sistema Nervoso"], list)
        assert "Ansiedade" in resultado["Sistema Nervoso"]

    def test_retorna_dict_vazio_para_super_inexistente(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_meso_and_micros_from_super("Categoria Inexistente")

        assert resultado == {}


class TestNormalizarCategoria:
    """Testes para o método normalizar_categoria."""

    def test_match_exato(self):
        wrapper = CategoriasWrapper()
        lista = ["Medicamentos", "Perfumaria e Cuidados"]
        resultado = wrapper.normalizar_categoria("Medicamentos", lista)

        assert resultado == "Medicamentos"

    def test_match_case_insensitive(self):
        wrapper = CategoriasWrapper()
        lista = ["Medicamentos", "Perfumaria e Cuidados"]
        resultado = wrapper.normalizar_categoria("medicamentos", lista)

        assert resultado == "Medicamentos"

    def test_retorna_none_para_categoria_inexistente(self):
        wrapper = CategoriasWrapper()
        lista = ["Medicamentos", "Perfumaria e Cuidados"]
        resultado = wrapper.normalizar_categoria("Inexistente", lista)

        assert resultado is None


class TestValidarENormalizarSuperCategoria:
    """Testes para o método validar_e_normalizar_super_categoria."""

    def test_valida_categoria_exata(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_super_categoria("Medicamentos")

        assert resultado == "Medicamentos"

    def test_valida_categoria_case_insensitive(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_super_categoria("medicamentos")

        assert resultado == "Medicamentos"

    def test_retorna_none_para_categoria_invalida(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_super_categoria("Inexistente")

        assert resultado is None


class TestValidarENormalizarMesoCategoria:
    """Testes para o método validar_e_normalizar_meso_categoria."""

    def test_valida_categoria_exata(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_meso_categoria(
            "Sistema Nervoso", "Medicamentos"
        )

        assert resultado == "Sistema Nervoso"

    def test_valida_categoria_case_insensitive(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_meso_categoria(
            "sistema nervoso", "Medicamentos"
        )

        assert resultado == "Sistema Nervoso"

    def test_retorna_none_para_categoria_invalida(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_meso_categoria(
            "Inexistente", "Medicamentos"
        )

        assert resultado is None


class TestValidarENormalizarMicroCategoria:
    """Testes para o método validar_e_normalizar_micro_categoria."""

    def test_valida_categoria_exata(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_micro_categoria(
            "Ansiedade", "Medicamentos", "Sistema Nervoso"
        )

        assert resultado == "Ansiedade"

    def test_valida_categoria_case_insensitive(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_micro_categoria(
            "ansiedade", "Medicamentos", "Sistema Nervoso"
        )

        assert resultado == "Ansiedade"

    def test_retorna_none_para_categoria_invalida(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_micro_categoria(
            "Inexistente", "Medicamentos", "Sistema Nervoso"
        )

        assert resultado is None

    def test_retorna_none_para_micro_none(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.validar_e_normalizar_micro_categoria(
            None, "Medicamentos", "Sistema Nervoso"
        )

        assert resultado is None


class TestGetOpcoesMesoMicroFormatadas:
    """Testes para o método get_opcoes_meso_micro_formatadas."""

    def test_retorna_string_formatada(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_opcoes_meso_micro_formatadas("Medicamentos")

        assert isinstance(resultado, str)
        assert "Sistema Nervoso -> Ansiedade" in resultado
        assert "- " in resultado  # Verifica que usa bullets

    def test_retorna_mensagem_para_super_inexistente(self):
        wrapper = CategoriasWrapper()
        resultado = wrapper.get_opcoes_meso_micro_formatadas("Inexistente")

        assert resultado == "Nenhuma categoria disponível"


class TestParseMesoMicroString:
    """Testes para o método parse_meso_micro_string."""

    def test_parse_formato_completo(self):
        resultado = CategoriasWrapper.parse_meso_micro_string(
            "Sistema Nervoso -> Ansiedade"
        )

        assert resultado == ("Sistema Nervoso", "Ansiedade")

    def test_parse_com_espacos_extras(self):
        resultado = CategoriasWrapper.parse_meso_micro_string(
            "  Sistema Nervoso  ->  Ansiedade  "
        )

        assert resultado == ("Sistema Nervoso", "Ansiedade")

    def test_raise_error_sem_seta(self):
        with pytest.raises(
            ValueError, match="Formato inválido.*Esperado 'Meso -> Micro'"
        ):
            CategoriasWrapper.parse_meso_micro_string("Sistema Nervoso")

    def test_raise_error_com_multiplas_setas(self):
        with pytest.raises(
            ValueError, match="Formato inválido.*Esperado exatamente uma seta"
        ):
            CategoriasWrapper.parse_meso_micro_string("A -> B -> C")


class TestParseMesoMicroLista:
    """Testes para o método parse_meso_micro_lista."""

    def test_parse_formato_completo(self):
        resultado = CategoriasWrapper.parse_meso_micro_lista(
            ["Sistema Nervoso -> Ansiedade", "Dor e Febre -> Analgésicos"]
        )

        assert len(resultado) == 2
        assert ("Sistema Nervoso", "Ansiedade") in resultado
        assert ("Dor e Febre", "Analgésicos") in resultado

    def test_parse_formato_apenas_meso(self):
        resultado = CategoriasWrapper.parse_meso_micro_lista(
            ["Sistema Nervoso", "Dor e Febre"]
        )

        assert len(resultado) == 2
        assert ("Sistema Nervoso", None) in resultado
        assert ("Dor e Febre", None) in resultado

    def test_parse_formato_misto(self):
        resultado = CategoriasWrapper.parse_meso_micro_lista(
            ["Sistema Nervoso -> Ansiedade", "Dor e Febre"]
        )

        assert len(resultado) == 2
        assert ("Sistema Nervoso", "Ansiedade") in resultado
        assert ("Dor e Febre", None) in resultado

    def test_ignora_strings_vazias(self):
        resultado = CategoriasWrapper.parse_meso_micro_lista(
            ["Sistema Nervoso -> Ansiedade", "", "  ", "Dor e Febre"]
        )

        assert len(resultado) == 2
        assert ("Sistema Nervoso", "Ansiedade") in resultado
        assert ("Dor e Febre", None) in resultado

    def test_ignora_formato_invalido(self):
        resultado = CategoriasWrapper.parse_meso_micro_lista(
            ["Sistema Nervoso -> Ansiedade", "A -> B -> C", "Dor e Febre"]
        )

        # "A -> B -> C" deve ser ignorado por formato inválido
        assert len(resultado) == 2
        assert ("Sistema Nervoso", "Ansiedade") in resultado
        assert ("Dor e Febre", None) in resultado

    def test_lista_vazia(self):
        resultado = CategoriasWrapper.parse_meso_micro_lista([])

        assert not resultado


class TestFlattenCategoria:
    """Testes para o método flatten_categoria."""

    def test_flatten_completo(self):
        resultado = CategoriasWrapper.flatten_categoria(
            "Medicamentos", "Sistema Nervoso", "Ansiedade"
        )

        assert resultado == "Medicamentos -> Sistema Nervoso -> Ansiedade"

    def test_flatten_sem_micro(self):
        resultado = CategoriasWrapper.flatten_categoria(
            "Medicamentos", "Sistema Nervoso", None
        )

        assert resultado == "Medicamentos -> Sistema Nervoso"

    def test_flatten_sem_super(self):
        resultado = CategoriasWrapper.flatten_categoria(
            None, "Sistema Nervoso", "Ansiedade"
        )

        assert resultado == "Sistema Nervoso -> Ansiedade"

    def test_flatten_apenas_meso(self):
        resultado = CategoriasWrapper.flatten_categoria(None, "Sistema Nervoso", None)

        assert resultado == "Sistema Nervoso"

    def test_flatten_com_tupla_micro(self):
        # Testa comportamento quando micro_categoria é tupla (primeiro elemento)
        resultado = CategoriasWrapper.flatten_categoria(
            "Medicamentos", "Sistema Nervoso", ("Ansiedade", True)
        )

        assert resultado == "Medicamentos -> Sistema Nervoso -> Ansiedade"


class TestArvoreCategoriasInstance:
    """Testes para a instância global arvore_categorias."""

    def test_instancia_global_existe(self):
        assert isinstance(arvore_categorias, dict)
        assert len(arvore_categorias) > 0
        assert "Medicamentos" in arvore_categorias
