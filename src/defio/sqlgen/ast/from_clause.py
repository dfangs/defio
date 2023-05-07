from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Set
from typing import final

from attrs import define
from typing_extensions import override

from defio.sql.ast.expression import BinaryExpression
from defio.sql.ast.from_clause import AliasedTable, FromClause, Join, JoinType
from defio.sql.ast.operator import BinaryOperator
from defio.sql.schema import Table
from defio.sqlgen.ast import GenSQL
from defio.sqlgen.ast.expression import GenColumnReference
from defio.sqlgen.ast.helper import UniqueTable
from defio.sqlgen.utils import sort_unique_tables


@define(frozen=True)
class GenFromClause(GenSQL):
    """Abstract base class wrapper for `FromClause`."""

    @override
    @abstractmethod
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> FromClause:
        raise NotImplementedError

    @property
    @abstractmethod
    def unique_tables(self) -> Set[UniqueTable]:
        raise NotImplementedError

    @final
    def generate_table_aliases(self) -> Mapping[UniqueTable, str]:
        # Group all unique tables based on their base table
        table_groups: dict[Table, set[UniqueTable]] = {}
        for unique_table in self.unique_tables:
            table_groups.setdefault(unique_table.table, set()).add(unique_table)

        # Create aliases if the same table is used more than once
        table_aliases = dict[UniqueTable, str]()
        for unique_tables in table_groups.values():
            if len(unique_tables) > 1:
                # Sort for deterministic results
                for i, unique_table in enumerate(sort_unique_tables(unique_tables)):
                    # Use 1-based indexing
                    table_aliases[unique_table] = f"{unique_table}_{i+1}"

        return table_aliases


@final
@define(frozen=True)
class GenAliasedTable(GenFromClause):
    """
    Wrapper class for `AliasedTable`.

    Note:
    Alias is left undetermined until the actual conversion to `SQL`, so that
    it can be assigned globally at once by taking into account other tables.
    """

    unique_table: UniqueTable

    @property
    @override
    def unique_tables(self) -> Set[UniqueTable]:
        return {self.unique_table}

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> AliasedTable:
        table_aliases = (
            table_aliases
            if table_aliases is not None
            else self.generate_table_aliases()
        )

        return AliasedTable(
            name=self.unique_table.name,
            alias=(
                table_aliases.get(self.unique_table)
                if table_aliases is not None
                else None
            ),
        )


@final
@define(frozen=True)
class GenJoin(GenFromClause):
    """
    Wrapper class for `Join`.

    Note:
    - `join_type` must not be `CROSS_JOIN`, since `predicate` cannot be `None`.
    - `right` is narrowed to `GenAliasedTable`, so that the join is left-deep.
    - `predicate` uses a specialized `JoinPredicate` class.
    """

    left: GenFromClause
    join_type: JoinType
    right: GenAliasedTable
    predicate: JoinPredicate

    def __attrs_post_init__(self) -> None:
        assert self.join_type is not JoinType.CROSS_JOIN
        assert (
            self.predicate.left.unique_table in self.unique_tables
            and self.predicate.right.unique_table in self.unique_tables
        )

    @property
    @override
    def unique_tables(self) -> Set[UniqueTable]:
        return self.left.unique_tables | self.right.unique_tables

    @override
    def to_sql(self, table_aliases: Mapping[UniqueTable, str] | None = None) -> Join:
        table_aliases = (
            table_aliases
            if table_aliases is not None
            else self.generate_table_aliases()
        )

        return Join(
            left=self.left.to_sql(table_aliases),
            join_type=self.join_type,
            right=self.right.to_sql(table_aliases),
            predicate=BinaryExpression(
                left=self.predicate.left.to_sql(table_aliases),
                operator=self.predicate.operator,
                right=self.predicate.right.to_sql(table_aliases),
            ),
        )


@final
@define(frozen=True)
class JoinPredicate:
    """
    Represents a join predicate over two tables (specifically, their columns).

    Intended as a replacement for `BinaryExpression` in `GenJoin`.
    """

    left: GenColumnReference
    operator: BinaryOperator
    right: GenColumnReference
