from __future__ import annotations

from enum import Enum, StrEnum


class UnaryOperator(StrEnum):
    """Unary operators for non-logical operations."""

    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    UNARY_PLUS = "+"
    NEGATION = "-"

    @staticmethod
    def from_symbol(symbol: str) -> UnaryOperator:
        """
        Returns a `UnaryOperator` with the corresponding symbol.

        Raises a `ValueError` if the symbol doesn't match any unary operator.
        """
        try:
            return UnaryOperator(symbol)
        except ValueError as exc:
            raise ValueError(f"`{symbol}` is not a valid UnaryOperator symbol") from exc


class BinaryOperator(Enum):
    """Binary operators for non-logical operations."""

    LT = ("<",)
    GT = (">",)
    LEQ = ("<=",)
    GEQ = (">=",)
    EQ = ("=",)
    NEQ = ("<>", "!=")
    IN = ("IN",)
    LIKE = ("LIKE", "~~")
    ILIKE = ("ILIKE", "!~~")
    BETWEEN = ("BETWEEN",)
    NOT_BETWEEN = ("NOT BETWEEN",)

    def __str__(self) -> str:
        return self.canonical_symbol

    @property
    def canonical_symbol(self) -> str:
        # Use the first symbol as the canonical representation
        return self.value[0]

    @staticmethod
    def from_symbol(symbol: str) -> BinaryOperator:
        """
        Returns a `BinaryOperator` with the corresponding symbol.

        Raises a `ValueError` if the symbol doesn't match any binary operator.
        """
        try:
            return next(op for op in BinaryOperator if symbol in op.value)
        except StopIteration as exc:
            raise ValueError(
                f"`{symbol}` is not a valid BinaryOperator symbol"
            ) from exc


class LogicalOperator(StrEnum):
    """Boolean/logical operators used in compound predicate."""

    AND = "AND"
    OR = "OR"
    NOT = "NOT"
