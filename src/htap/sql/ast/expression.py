from collections.abc import Sequence
from typing import assert_never

from attrs import define, field
from typing_extensions import override

from htap.sql.ast import SQL
from htap.sql.operator import BinaryOperator, UnaryOperator
from htap.utils.attrs import to_tuple


@define(frozen=True)
class Expression(SQL):
    """Abstract base class for expressions in SQL."""


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


@define(frozen=True)
class BinaryExpression(Expression):
    """Represents a binary operation."""

    operator: BinaryOperator
    left: Expression
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


@define(frozen=True)
class FunctionCall(Expression):
    """Represents a function call (e.g., aggregate functions)."""

    func_name: str
    args: Sequence[Expression] = field(converter=to_tuple)

    @override
    def __str__(self) -> str:
        return f"{self.func_name}({', '.join(str(arg) for arg in self.args)})"
