from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from typing import final

from attrs import define, field
from typing_extensions import override

from defio.sql.ast.statement import SelectStatement, Statement, TargetList
from defio.sqlgen.ast import GenSQL
from defio.sqlgen.ast.expression import GenExpression
from defio.sqlgen.ast.from_clause import GenFromClause
from defio.sqlgen.ast.helper import UniqueTable
from defio.sqlgen.ast.where_clause import GenWhereClause
from defio.utils.attrs import to_tuple


@define(frozen=True)
class GenStatement(GenSQL):
    """Abstract base class wrapper for `Statement`."""

    @override
    @abstractmethod
    def to_sql(self) -> Statement:
        raise NotImplementedError


@final
@define(frozen=True)
class GenSelectStatement(GenStatement):
    """
    Wrapper class for `SelectStatement`.

    Note: `from_clause` is no longer allowed to be `None`.
    """

    target_list: GenTargetList
    from_clause: GenFromClause
    where_clause: GenWhereClause | None

    @override
    def to_sql(self) -> SelectStatement:
        table_aliases = self.from_clause.generate_table_aliases()

        return SelectStatement(
            target_list=self.target_list.to_sql(table_aliases),
            from_clause=self.from_clause.to_sql(table_aliases),
            where_clause=(
                self.where_clause.to_sql(table_aliases)
                if self.where_clause is not None
                else None
            ),
        )


@final
@define(frozen=True)
class GenTargetList(GenSQL):
    """Wrapper class for `TargetList`."""

    targets: Sequence[GenExpression] = field(converter=to_tuple)

    @override
    def to_sql(
        self, table_aliases: Mapping[UniqueTable, str] | None = None
    ) -> TargetList:
        return TargetList(
            targets=[target.to_sql(table_aliases) for target in self.targets]
        )
