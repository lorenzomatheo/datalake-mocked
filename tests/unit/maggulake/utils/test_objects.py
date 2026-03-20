import pytest

from maggulake.utils.objects import remove_empty_elements


class TestRemoveVaziosDeUmObjeto:
    """Testa a remoção recursiva de elementos nulos em um objeto (dict, list, int, etc...)"""

    @pytest.mark.parametrize(
        "input_data, expected",
        [
            # dict simples
            ({"a": 1, "b": None, "c": ""}, {"a": 1}),
            ({"a": {}, "b": [], "c": 3}, {"c": 3}),
            # list simples
            ([1, None, "", [], 2], [1, 2]),
            # aninhado misto
            (
                {
                    "keep": {"nested": {"deep": 42}},
                    "drop": {"nested": {}},
                    "list": [1, [], {"x": None}, {"y": 5}],
                },
                {"keep": {"nested": {"deep": 42}}, "list": [1, {"y": 5}]},
            ),
            # lista de dicts
            (
                [{"id": 1, "val": None}, {"id": 2, "val": 2}, {}],
                [{"id": 1}, {"id": 2, "val": 2}],
            ),
            # vazio completo
            ({}, {}),
            ([], []),
            (None, None),
            # apenas valores "vazios"
            ([None, "", {}, []], []),
            ({"a": None, "b": "", "c": {}, "d": []}, {}),
            # valores falsos mas válidos
            ({"zero": 0, "false": False, "empty_str": ""}, {"zero": 0, "false": False}),
            # profundamente aninhado
            (
                {"l1": {"l2": {"l3": {"l4": {}}}}},
                {},
            ),
            # strings com espaços não são consideradas vazias
            ({"sp": " ", "empty": ""}, {"sp": " "}),
            # estrutura com listas vazias aninhadas
            ([[[], []], [1, []], []], [[1]]),
        ],
    )
    def test_remove_empty_elements(self, input_data, expected):
        assert remove_empty_elements(input_data) == expected
