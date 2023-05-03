from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from typing import TypeAlias, final

from attrs import define, field
from typing_extensions import override

from defio.sql.ast.expression import (
    BinaryExpression,
    ColumnReference,
    Constant,
    Expression,
    FunctionCall,
    FunctionName,
    UnaryExpression,
)
from defio.sql.ast.operator import BinaryOperator, UnaryOperator
from defio.sql.schema import Column
from defio.sqlgen.ast import GenSQL
from defio.sqlgen.ast.helper import UniqueTable
from defio.utils.attrs import to_tuple

ConstantType: TypeAlias = int | float | str | bool
ConstantSequenceType: TypeAlias = (
    Sequence[int] | Sequence[float] | Sequence[str] | Sequence[bool]
)


@define(frozen=True)
class GenExpression(GenSQL):
    """Abstract base class wrapper for `Expression`."""

    @override
    @abstractmethod
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> Expression:
        raise NotImplementedError


@final
@define(frozen=True)
class GenUnaryExpression(GenExpression):
    """Wrapper class for `UnaryExpression`."""

    operator: UnaryOperator
    operand: GenExpression

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> UnaryExpression:
        return UnaryExpression(
            operator=self.operator, operand=self.operand.to_sql(table_aliases)
        )


@final
@define(frozen=True)
class GenBinaryExpression(GenExpression):
    """Wrapper class for `BinaryExpression`."""

    left: GenExpression
    operator: BinaryOperator
    right: GenExpression | Sequence[GenExpression] = field(converter=to_tuple)

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> BinaryExpression:
        return BinaryExpression(
            left=self.left.to_sql(table_aliases),
            operator=self.operator,
            right=(
                [gen_expr.to_sql(table_aliases) for gen_expr in self.right]
                if isinstance(self.right, Sequence)
                else self.right.to_sql(table_aliases)
            ),
        )


@final
@define(frozen=True)
class GenColumnReference(GenExpression):
    """Wrapper class for `ColumnReference`."""

    unique_table: UniqueTable
    column: Column

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> ColumnReference:
        return ColumnReference(
            table_alias=(
                table_aliases[self.unique_table]
                if table_aliases is not None and self.unique_table in table_aliases
                else self.unique_table.name
            ),
            column_name=self.column.name,
        )


@final
@define(frozen=True)
class GenConstant(GenExpression):
    """Wrapper class for `Constant`."""

    value: ConstantType

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> Constant:
        return Constant(value=self.value)


@final
@define(frozen=True)
class GenFunctionCall(GenExpression):
    """Wrapper class for `FunctionCall`."""

    func_name: FunctionName
    agg_star: bool = False
    args: Sequence[GenExpression] | None = field(default=None, converter=to_tuple)

    def __attrs_post_init__(self) -> None:
        assert (self.agg_star and self.args is None) or (
            not self.agg_star and self.args is not None and len(self.args) > 0
        )

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> FunctionCall:
        if self.args is None:
            return FunctionCall(func_name=self.func_name, agg_star=True)
        return FunctionCall(
            func_name=self.func_name,
            args=[gen_expr.to_sql(table_aliases) for gen_expr in self.args],
        )
