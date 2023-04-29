from __future__ import annotations

from enum import StrEnum, unique

from attrs import define
from typing_extensions import override

from defio.sql.ast import SQL
from defio.sql.ast.expression import Expression


@define(frozen=True)
class FromClause(SQL):
    """Abstract base class for `from_clause` of a `SELECT` statement."""


@define(frozen=True)
class AliasedTable(FromClause):
    """Represents a table (which may have an alias name)."""

    name: str
    alias: str | None

    @override
    def __str__(self) -> str:
        if self.alias is not None:
            return f"{self.name} AS {self.alias}"
        return self.name


@define(frozen=True)
class Join(FromClause):
    """
    Represents a join expression of two `from_clause` items.

    Note that the join `predicate` may be `None` (e.g., cross joins).
    """

    join_type: JoinType
    left: FromClause
    right: FromClause
    predicate: Expression | None

    def __attrs_post_init__(self) -> None:
        assert (self.predicate is None and self.join_type is JoinType.CROSS_JOIN) or (
            self.predicate is not None and self.join_type is not JoinType.CROSS_JOIN
        )

    @override
    def __str__(self) -> str:
        if self.join_type is JoinType.CROSS_JOIN:
            return f"{self.left}, {self.right}"

        assert self.predicate is not None
        return f"{self.left} {self.join_type} {self.right} ON {self.predicate}"


@unique
class JoinType(StrEnum):
    """Join types supported by PostgreSQL."""

    INNER_JOIN = "JOIN"
    LEFT_OUTER_JOIN = "LEFT OUTER JOIN"
    RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN"
    FULL_OUTER_JOIN = "FULL OUTER JOIN"
    CROSS_JOIN = "CROSS JOIN"
