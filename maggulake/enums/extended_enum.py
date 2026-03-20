from enum import Enum


class ExtendedEnum(Enum):
    # https://stackoverflow.com/questions/29503339/how-to-get-all-values-from-python-enum-class

    @classmethod
    def list(cls) -> list[str]:
        return list(map(lambda c: c.value, cls))

    @classmethod
    def tuple(cls) -> tuple[str, ...]:
        return tuple(map(lambda c: c.value, cls))
