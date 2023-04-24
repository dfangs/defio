from __future__ import annotations

import json
from collections.abc import Sequence, Set
from enum import Enum, unique
from typing import Any, TextIO, final

import pandas as pd
from attrs import define, field

from htap.utils.attrs import to_tuple
from htap.utils.graph import DirectedGraph


@final
@define(frozen=True)
class Schema:
    """Schema of a dataset."""

    name: str
    tables: Sequence[Table] = field(converter=to_tuple)
    relationships: RelationshipGraph

    def get_table(self, table_name: str) -> Table:
        """
        Returns a table with the given name.

        Raises a `ValueError` if the table doesn't exist in this schema.
        """
        try:
            return next(table for table in self.tables if table.name == table_name)
        except StopIteration as exc:
            raise ValueError(f"Table `{table_name}` does not exist") from exc

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Schema:
        """Converts the given dict/JSON representation into a schema."""
        return Schema(
            name=data["name"],
            tables=(tables := [Table.from_dict(table) for table in data["tables"]]),
            relationships=RelationshipGraph.from_list(data["relationships"], tables),
        )

    def to_dict(self) -> dict[str, Any]:
        """Converts this schema into its dict/JSON representation."""
        return {
            "name": self.name,
            "tables": [table.to_dict() for table in self.tables],
            "relationships": self.relationships.to_list(),
        }

    @staticmethod
    def load(f: TextIO) -> Schema:
        """Loads the given file into a schema."""
        return Schema.from_dict(json.load(f))

    def dump(self, f: TextIO) -> None:
        """Serializes this schema and writes it into the given file."""
        json.dump(self.to_dict(), f)


@final
@define(frozen=True)
class RelationshipGraph:
    """
    Relationship graph of the tables in a dataset.
    """

    _graph: DirectedGraph[TableColumn]
    _reverse_graph: DirectedGraph[TableColumn]

    def __init__(
        self,
        tables: Sequence[Table],
        relationships: Sequence[tuple[Table, Column, Table, Column]],
    ) -> None:
        """
        Initializes this relationship graph.

        Each tuple `r` in `relationships` represents a foreign-key reference
        from table-column (`r[0]`, `r[1]`) to table-column (`r[2]`, `r[3]`).
        """
        nodes = {
            table_column for table in tables for table_column in table.table_columns
        }

        edges = {
            (TableColumn(from_table, from_column), TableColumn(to_table, to_column))
            for from_table, from_column, to_table, to_column in relationships
        }

        reverse_edges = {(to_node, from_node) for from_node, to_node in edges}

        # Cannot use normal field assignment for frozen dataclasses
        # Reference: https://docs.python.org/3/library/dataclasses.html#frozen-instances
        object.__setattr__(self, "_graph", DirectedGraph[TableColumn](nodes, edges))
        object.__setattr__(
            self, "_reverse_graph", DirectedGraph[TableColumn](nodes, reverse_edges)
        )

    def get_possible_joins(self, table: Table, column: Column) -> Set[TableColumn]:
        """
        Returns all table-column pairs that can be equijoined with the given
        table-column pair based on this relationship graph.

        Raises a `ValueError` if the table-column pair doesn't exist in this graph.
        """
        table_column = TableColumn(table, column)
        if table_column not in self._graph.nodes:
            raise ValueError(
                f"Table-column pair `{table.name}.{column.name}` does not exist"
            )

        forward_neighbors = self._graph.get_neighbors(TableColumn(table, column))
        reverse_neighbors = self._reverse_graph.get_neighbors(
            TableColumn(table, column)
        )

        return forward_neighbors | reverse_neighbors

    @staticmethod
    def from_list(data: list[list[str]], tables: Sequence[Table]) -> RelationshipGraph:
        """Converts the given array/JSON representation into a relationship graph."""
        tables_by_name = {table.name: table for table in tables}
        edges = [
            (
                (from_table := tables_by_name[from_table_name]),
                from_table.get_column(from_column_name),
                (to_table := tables_by_name[to_table_name]),
                to_table.get_column(to_column_name),
            )
            for (
                from_table_name,
                from_column_name,
                to_table_name,
                to_column_name,
            ) in data
        ]

        return RelationshipGraph(tables, edges)

    def to_list(self) -> list[list[str]]:
        """
        Converts this relationship graph into its array/JSON representation.

        The returned list is sorted in lexicographical order.
        """
        # Temporarily use tuples to sort values
        sorted_list = sorted(
            (
                from_node.table.name,
                from_node.column.name,
                to_node.table.name,
                to_node.column.name,
            )
            for from_node, to_node in self._graph.edges
        )

        return [list(item) for item in sorted_list]


@final
@define(frozen=True)
class TableColumn:
    """Record type that represents a particular table-column pair."""

    table: Table
    column: Column


@final
@define(frozen=True)
class Table:
    """Table/relation of a dataset."""

    name: str
    columns: Sequence[Column] = field(converter=to_tuple)

    @property
    def table_columns(self) -> Sequence[TableColumn]:
        """Convenience method that returns all table-column pairs in this table."""
        return [TableColumn(self, column) for column in self.columns]

    def get_column(self, column_name: str) -> Column:
        """
        Returns a column with the given name

        Raises a `ValueError` if the column doesn't exist in this table.
        """
        try:
            return next(column for column in self.columns if column.name == column_name)
        except StopIteration as exc:
            raise ValueError(f"Column `{column_name}` does not exist") from exc

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Table:
        """Converts this given dict/JSON representation into a table."""
        return Table(
            name=data["name"],
            columns=[Column.from_dict(column) for column in data["columns"]],
        )

    def to_dict(self) -> dict[str, Any]:
        """Converts this table into its dict/JSON representation."""
        return {
            "name": self.name,
            "columns": [column.to_dict() for column in self.columns],
        }


@final
@define(frozen=True)
class Column:
    """Column of a table/relation."""

    name: str
    dtype: DataType
    constraint: ColumnConstraint

    @property
    def is_unique(self) -> bool:
        """Returns whether this column only contains unique values."""
        return self.constraint.is_unique or self.constraint.is_primary_key

    @property
    def is_not_null(self) -> bool:
        """Returns whether this column only contains non-null values."""
        return self.constraint.is_not_null or self.constraint.is_primary_key

    @staticmethod
    def from_dict(data: dict[str, Any]) -> Column:
        """Converts the given dict/JSON representation into a column."""
        return Column(
            name=data["name"],
            dtype=DataType.from_str(data["dtype"]),
            constraint=ColumnConstraint(
                is_primary_key=data["is_primary_key"],
                is_unique=data["is_unique"],
                is_not_null=data["is_not_null"],
                max_char_length=data["max_char_length"],
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Converts this column into its dict/JSON representation."""
        return {
            "name": self.name,
            "dtype": str(self.dtype),
            "is_primary_key": self.constraint.is_primary_key,
            "is_unique": self.constraint.is_unique,
            "is_not_null": self.constraint.is_not_null,
            "max_char_length": self.constraint.max_char_length,
        }


@unique
class DataType(Enum):
    """Data types supported by PostgreSQL."""

    INTEGER = ("integer", "pg_catalog.int4", pd.Int32Dtype())
    FLOAT = ("real", "pg_catalog.float4", pd.Float64Dtype())
    STRING = ("character varying", "pg_catalog.varchar", pd.StringDtype())
    BOOLEAN = ("boolean", "pg_catalog.bool", pd.BooleanDtype())

    def __init__(self, common_name: str, internal_name: str, pandas_dtype: Any) -> None:
        # Note: The parentheses in the `pandas_dtype` constructors are required
        self.common_name = common_name
        self.internal_name = internal_name
        self.pandas_dtype = pandas_dtype

    def __str__(self) -> str:
        return self.common_name

    @staticmethod
    def from_str(name: str) -> DataType:
        """
        Creates a DataType with the given common name or internal name.

        Raises a `ValueError` if the input doesn't match any `DataType`.
        """
        for dtype in DataType:
            if name in {dtype.common_name, dtype.internal_name}:
                return dtype
        raise ValueError(f"`{name}` does not correspond to any DataType")


@final
@define(frozen=True)
class ColumnConstraint:
    """
    Possible constraints of a table's column, including the type mods
    (e.g., `n` in `varchar(n)`).

    Note that this does not include foreign key constraint.
    """

    is_primary_key: bool
    is_unique: bool
    is_not_null: bool
    max_char_length: int | None = None
