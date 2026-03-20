from types import SimpleNamespace

from maggulake.pipelines.process_invalid_compliance_columns import (
    process_invalid_columns,
)


class TestProcessInvalidColumns:
    def test_corrects_all_known_columns_using_fuzzy_matching(self, monkeypatch):
        row = SimpleNamespace(
            brand="brnd incorreto",
            form="capsulas",
        )
        column_to_enum = {
            "brand": ["Marca Correta"],
            "form": ["Forma Correta"],
        }

        calls: list[tuple[str | None, tuple[str, ...]]] = []

        def fake_fuzzy(value, choices):
            calls.append((value, tuple(choices)))
            return choices[0]

        monkeypatch.setattr(
            "maggulake.pipelines.process_invalid_compliance_columns.fuzzy_matching",
            fake_fuzzy,
        )

        corrected = process_invalid_columns(row, column_to_enum)

        assert corrected == {"brand": "Marca Correta", "form": "Forma Correta"}
        assert calls == [
            ("brnd incorreto", ("Marca Correta",)),
            ("capsulas", ("Forma Correta",)),
        ]

    def test_handles_missing_attributes(self, monkeypatch):
        # Row sem o atributo brand - getattr retorna None
        row = SimpleNamespace()
        column_to_enum = {"brand": ["Marca Correta"]}

        values_seen: list[str | None] = []

        def fake_fuzzy(value, _choices):
            values_seen.append(value)
            return "Resultado"

        monkeypatch.setattr(
            "maggulake.pipelines.process_invalid_compliance_columns.fuzzy_matching",
            fake_fuzzy,
        )

        corrected = process_invalid_columns(row, column_to_enum)

        # Quando o valor é None, não deve chamar fuzzy_matching
        assert not corrected
        assert not values_seen

    def test_ignores_columns_not_in_column_to_enum(self, monkeypatch):
        # Row tem um atributo extra que não está no column_to_enum
        row = SimpleNamespace(brand="brnd incorreto", unknown_field="valor qualquer")
        column_to_enum = {"brand": ["Marca Correta"]}

        calls = []

        def fake_fuzzy(value, choices):
            calls.append((value, choices))
            return "Resultado"

        monkeypatch.setattr(
            "maggulake.pipelines.process_invalid_compliance_columns.fuzzy_matching",
            fake_fuzzy,
        )

        corrected = process_invalid_columns(row, column_to_enum)

        # Deve processar apenas a coluna brand
        assert corrected == {"brand": "Resultado"}
        assert len(calls) == 1
        assert calls[0][0] == "brnd incorreto"

    def test_does_not_correct_valid_enum_values(self, monkeypatch):
        # Row com valor que já está no enum
        row = SimpleNamespace(brand="Marca Correta")
        column_to_enum = {"brand": ["Marca Correta", "Outra Marca"]}

        calls = []

        def fake_fuzzy(_value, choices):
            calls.append((None, choices))
            return "Nunca deve chegar aqui"

        monkeypatch.setattr(
            "maggulake.pipelines.process_invalid_compliance_columns.fuzzy_matching",
            fake_fuzzy,
        )

        corrected = process_invalid_columns(row, column_to_enum)

        # Valor já é válido, não deve ser corrigido
        assert not corrected
        assert len(calls) == 0

    def test_handles_fuzzy_matching_returning_none(self, monkeypatch):
        # Fuzzy matching pode retornar None se não encontrar match
        row = SimpleNamespace(brand="valor totalmente diferente")
        column_to_enum = {"brand": ["Marca Correta"]}

        def fake_fuzzy(_value, _choices):
            return None

        monkeypatch.setattr(
            "maggulake.pipelines.process_invalid_compliance_columns.fuzzy_matching",
            fake_fuzzy,
        )

        corrected = process_invalid_columns(row, column_to_enum)

        # Se fuzzy_matching retorna None, não deve incluir no resultado
        assert not corrected
