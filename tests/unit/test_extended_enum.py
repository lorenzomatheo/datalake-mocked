from maggulake.enums import ExtendedEnum


def test_extended_enum():
    class TestEnum(ExtendedEnum):
        A = 1
        B = 2
        C = 3

    assert TestEnum.list() == [1, 2, 3]
    assert TestEnum.tuple() == (1, 2, 3)
