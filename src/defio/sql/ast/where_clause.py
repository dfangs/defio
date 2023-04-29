from __future__ import annotations

from collections.abc import Sequence

from attrs import define, field
from typing_extensions import override

from defio.sql.ast import SQL
from defio.sql.ast.expression import Expression
from defio.sql.operator import LogicalOperator
from defio.utils.attrs import to_tuple


@define(frozen=True)
class WhereClause(SQL):
    """Abstract base class for `where_clause` of a `SELECT` statement."""


@define(frozen=True)
class SimplePredicate(WhereClause):
    """Represents a single unary/binary predicate."""

    expression: Expression

    @override
    def __str__(self) -> str:
        return str(self.expression)


@define(frozen=True)
class CompoundPredicate(WhereClause):
    """Represents a compound predicate with an AND/OR/NOT operator."""

    operator: LogicalOperator
    children: Sequence[WhereClause] = field(converter=to_tuple)

    def __attrs_post_init__(self) -> None:
        # Pylance doesn't support attrs' decorator-based validator yet
        # See https://github.com/python-attrs/attrs/issues/795
        assert (self.operator is LogicalOperator.NOT and len(self.children) == 1) or (
            self.operator is not LogicalOperator.NOT and len(self.children) > 1
        )

    @override
    def __str__(self) -> str:
        def parenthesize(item: WhereClause) -> str:
            # Hacky way to enfore operator precedence
            match item:
                case SimplePredicate():
                    return str(item)
                case CompoundPredicate():
                    return f"({item})"
                case _:
                    raise RuntimeError("Should not reach here")

        if self.operator is LogicalOperator.NOT:
            assert len(self.children) == 1
            return f"{self.operator} {self.children[0]}"

        return f" {self.operator} ".join(parenthesize(child) for child in self.children)
