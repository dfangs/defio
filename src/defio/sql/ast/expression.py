from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum, auto, unique
from typing import assert_never, final

from attrs import define, field
from typing_extensions import override

from defio.sql.ast import SQL
from defio.sql.operator import BinaryOperator, UnaryOperator
from defio.utils.attrs import to_tuple


@define(frozen=True)
class Expression(SQL):
    """Abstract base class for expressions in SQL."""


@final
@define(frozen=True)
class UnaryExpression(Expression):
    """Represents a unary operation."""

    operator: UnaryOperator
    operand: Expression

    @override
    def __str__(self) -> str:  # pylint: disable=invalid-str-returned
        # Need to differentiate left & right unary operators
        match self.operator:
            case UnaryOperator.IS_NULL | UnaryOperator.IS_NOT_NULL:
                return f"{self.operand} {self.operator}"
            case UnaryOperator.UNARY_PLUS | UnaryOperator.NEGATION:
                return f"{self.operator}{self.operand}"  # No space in-between
            case _:
                assert_never(self.operator)


@final
@define(frozen=True)
class BinaryExpression(Expression):
    """Represents a binary operation."""

    left: Expression
    operator: BinaryOperator
    right: Expression | Sequence[Expression] = field(converter=to_tuple)

    @override
    def __str__(self) -> str:
        right_str: str
        match self.operator:
            case BinaryOperator.IN:
                assert isinstance(self.right, Sequence)
                right_str = f"({', '.join(str(expr) for expr in self.right)})"

            case BinaryOperator.BETWEEN | BinaryOperator.NOT_BETWEEN:
                assert isinstance(self.right, Sequence) and len(self.right) == 2
                right_str = f"{self.right[0]} AND {self.right[1]}"

            case _:
                right_str = str(self.right)

        return f"{self.left} {self.operator.canonical_symbol} {right_str}"


@final
@define(frozen=True)
class ColumnReference(Expression):
    """Represents a particular column of a table."""

    table_alias: str | None
    column_name: str

    @override
    def __str__(self) -> str:
        # NOTE: We don't handle quoted identifiers here
        if self.table_alias is not None:
            return f"{self.table_alias}.{self.column_name}"
        return self.column_name


@final
@define(frozen=True)
class Constant(Expression):
    """Represents a literal value."""

    value: int | float | str | bool

    @override
    def __str__(self) -> str:  # pylint: disable=invalid-str-returned
        match self.value:
            case int():
                return str(self.value)
            case float():
                return str(self.value)
            case str():
                return f"'{self.value}'"
            case bool():
                return str(self.value)
            case _:
                assert_never(self.value)


@unique
class FunctionName(StrEnum):
    """Built-in functions supported by PostgreSQL."""

    COUNT = auto()
    MIN = auto()
    MAX = auto()
    SUM = auto()
    AVG = auto()

    @staticmethod
    def from_str(case_insensitive_name: str) -> FunctionName:
        """
        Returns a function name that matches the given input string
        while ignoring the case.

        Raises a `ValueError` if the input doesn't match any `FunctionName`.
        """
        for func_name in FunctionName:
            if case_insensitive_name.casefold() == func_name.casefold():
                return func_name
        raise ValueError(
            f"`{case_insensitive_name}` does not correspond to any FunctionName"
        )


@final
@define(frozen=True)
class FunctionCall(Expression):
    """Represents a function call (e.g., aggregate functions)."""

    func_name: FunctionName
    agg_star: bool = False
    args: Sequence[Expression] | None = field(default=None, converter=to_tuple)

    def __attrs_post_init__(self) -> None:
        assert (self.agg_star and self.args is None) or (
            not self.agg_star and self.args is not None and len(self.args) > 0
        )

    @override
    def __str__(self) -> str:
        if self.args is None:
            return f"{self.func_name.upper()}(*)"
        return f"{self.func_name.upper()}({', '.join(str(arg) for arg in self.args)})"
