from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from typing import final

from attrs import define, field
from typing_extensions import override

from defio.sql.ast.operator import BinaryOperator, LogicalOperator, UnaryOperator
from defio.sql.ast.where_clause import CompoundPredicate, SimplePredicate, WhereClause
from defio.sqlgen.ast import GenSQL
from defio.sqlgen.ast.expression import (
    ConstantSequenceType,
    ConstantType,
    GenBinaryExpression,
    GenColumnReference,
    GenConstant,
    GenExpression,
    GenUnaryExpression,
)
from defio.sqlgen.ast.helper import UniqueTable
from defio.utils.attrs import to_tuple


@define(frozen=True)
class GenWhereClause(GenSQL):
    """Abstract base class wrapper for `WhereClause`."""

    @override
    @abstractmethod
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> WhereClause:
        raise NotImplementedError


@final
@define(frozen=True)
class GenSimplePredicate(GenWhereClause):
    """Wrapper class for `SimplePredicate`."""

    expression: GenExpression

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> SimplePredicate:
        return SimplePredicate(expression=self.expression.to_sql(table_aliases))

    @staticmethod
    def make_unary_column_predicate(
        operator: UnaryOperator, operand: GenColumnReference
    ) -> GenSimplePredicate:
        return GenSimplePredicate(
            expression=GenUnaryExpression(operator=operator, operand=operand)
        )

    @staticmethod
    def make_binary_column_predicate(
        left: GenColumnReference,
        operator: BinaryOperator,
        right: ConstantType | ConstantSequenceType,
    ) -> GenSimplePredicate:
        return GenSimplePredicate(
            expression=GenBinaryExpression(
                left=left,
                operator=operator,
                right=(
                    GenConstant(right)
                    # Note: Pylance doesn't support narrowing with `typing.get_args()`
                    if isinstance(right, (int, float, str, bool))
                    else [GenConstant(value=const) for const in right]
                ),
            )
        )


@final
@define(frozen=True)
class GenCompoundPredicate(GenWhereClause):
    """Wrapper class for `CompoundPredicate`."""

    operator: LogicalOperator
    children: Sequence[GenWhereClause] = field(converter=to_tuple)

    def __attrs_post_init__(self) -> None:
        assert (self.operator is LogicalOperator.NOT and len(self.children) == 1) or (
            self.operator is not LogicalOperator.NOT and len(self.children) > 1
        )

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> CompoundPredicate:
        return CompoundPredicate(
            operator=self.operator,
            children=[child.to_sql(table_aliases) for child in self.children],
        )

    @staticmethod
    def make_and(operands: Sequence[GenWhereClause]) -> GenCompoundPredicate:
        return GenCompoundPredicate(operator=LogicalOperator.AND, children=operands)

    @staticmethod
    def make_or(operands: Sequence[GenWhereClause]) -> GenCompoundPredicate:
        return GenCompoundPredicate(operator=LogicalOperator.OR, children=operands)

    @staticmethod
    def make_not(operand: GenWhereClause) -> GenCompoundPredicate:
        return GenCompoundPredicate(operator=LogicalOperator.NOT, children=[operand])
