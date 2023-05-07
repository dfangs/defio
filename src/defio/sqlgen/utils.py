from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from defio.sql.schema import TableColumn
from defio.sqlgen.ast.helper import UniqueTable

if TYPE_CHECKING:
    from defio.sqlgen.sampler.join import JoinEdge


def sort_join_edges(join_edges: Iterable[JoinEdge]) -> Sequence[JoinEdge]:
    """Helper function to produce a deterministic ordering of join edges."""

    # This is sufficient, since tables must have unique names
    def stringify(table_column: TableColumn) -> str:
        table, column = table_column
        return f"{table.name}.{column.name}"

    return list(
        sorted(
            join_edges,
            key=lambda join_edge: tuple(
                sorted((stringify(join_edge.first), stringify(join_edge.second)))
            ),
        )
    )


def sort_unique_tables(
    unique_tables: Iterable[UniqueTable],
) -> Sequence[UniqueTable]:
    """Helper function to produce a deterministic ordering of unique tables."""
    return list(
        sorted(
            unique_tables,
            key=lambda unique_table: (unique_table.table.name, id(unique_table)),
        )
    )
