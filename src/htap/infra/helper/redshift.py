from enum import StrEnum, auto, unique


@unique
class RedshiftNodeType(StrEnum):
    @staticmethod
    def _generate_next_value_(
        name: str, start: str, count: int, last_values: list[str]
    ) -> str:
        return name.lower().replace("_", ".")

    RA3_XLPLUS = auto()
    RA3_4XLARGE = auto()
    RA3_16XLARGE = auto()
    DS2_XLARGE = auto()
    DS2_8XLARGE = auto()
    DC2_LARGE = auto()
    DC2_8XLARGE = auto()
    DC1_LARGE = auto()
    DC1_8XLARGE = auto()
