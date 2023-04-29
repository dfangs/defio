from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import StrEnum, unique

from attrs import define, field
from typing_extensions import override

from htap.sql.ast import SQL
from htap.sql.ast.expression import Expression
from htap.sql.ast.from_clause import FromClause
from htap.sql.ast.where_clause import WhereClause
from htap.sql.schema import Table
from htap.utils.attrs import to_map, to_tuple


@define(frozen=True)
class Statement(SQL):
    """Abstract base class for SQL statements."""


@define(frozen=True)
class CreateStatement(Statement):
    """Represents a SQL `CREATE` statement."""

    table: Table
    fk_references: Mapping[str, tuple[str, str]] = field(converter=to_map)

    @override
    def __str__(self) -> str:
        column_defs: list[str] = []
        for column in self.table.columns:
            column_def_parts = [column.name]
            constraint = column.constraint

            if constraint.max_char_length is not None:
                column_def_parts.append(
                    f"{column.dtype.common_name}({constraint.max_char_length})"
                )
            else:
                column_def_parts.append(column.dtype.common_name)

            if constraint.is_primary_key:
                column_def_parts.append("PRIMARY KEY")
            if constraint.is_unique:
                column_def_parts.append("UNIQUE")
            if constraint.is_not_null:
                column_def_parts.append("NOT NULL")

            if column.name in self.fk_references:
                fk_table, fk_column = self.fk_references[column.name]
                column_def_parts.append(f"REFERENCES {fk_table}({fk_column})")

            column_defs.append(" ".join(column_def_parts))

        return f"CREATE TABLE {self.table.name} ({', '.join(column_defs)});"


@define(frozen=True)
class DropStatement(Statement):
    """Represents a SQL `DROP` statement."""

    remove_type: DropRemoveType
    is_cascade: bool
    is_missing_ok: bool
    objects: Sequence[str] = field(converter=to_tuple)

    @override
    def __str__(self) -> str:
        return (
            f"DROP {self.remove_type} "
            + ("IF EXISTS " if self.is_missing_ok else "")
            + ", ".join(self.objects)
            + ";"
        )


@unique
class DropRemoveType(StrEnum):
    """Types of objects that can be removed via `DROP` statements."""

    TABLE = "TABLE"
    COLUMN = "COLUMN"


@define(frozen=True)
class SelectStatement(Statement):
    """Represents a SQL `SELECT` statement."""

    target_list: TargetList
    from_clause: FromClause | None
    where_clause: WhereClause | None

    @override
    def __str__(self) -> str:
        return (
            f"SELECT {self.target_list}"
            + (f" FROM {self.from_clause}" if self.from_clause is not None else "")
            + (f" WHERE {self.where_clause}" if self.where_clause is not None else "")
            + ";"
        )


@define(frozen=True)
class TargetList(SQL):
    """Represents a target list of a `SELECT` statement."""

    targets: Sequence[Expression] = field(converter=to_tuple)

    @override
    def __str__(self) -> str:
        return ", ".join(str(target) for target in self.targets)
