from __future__ import annotations

import json
import multiprocessing as mp
from collections.abc import Mapping
from itertools import repeat
from typing import Any, TextIO, TypeAlias, final, overload

import pandas as pd
from attrs import define, field

from defio.dataset import Dataset
from defio.dataset.column_stats import (
    CategoricalColumnStats,
    ColumnStats,
    KeyColumnStats,
    NumericalColumnStats,
    RawStringColumnStats,
)
from defio.sql.schema import Column, Table
from defio.utils.logging import log_around
from defio.utils.time import measure_time

# Union types for pattern matching

IntegerColumnStats: TypeAlias = (
    CategoricalColumnStats[int] | KeyColumnStats[int] | NumericalColumnStats
)

FloatColumnStats: TypeAlias = NumericalColumnStats

StringColumnStats: TypeAlias = (
    CategoricalColumnStats[str] | KeyColumnStats[str] | RawStringColumnStats
)

BooleanColumnStats: TypeAlias = CategoricalColumnStats[bool]


@final
@define
class TableStats:
    """Container for the statistics of all columns in a table."""

    _stats: Mapping[Column, ColumnStats] = field(alias="stats")

    @overload
    def get(self, column: Column, /) -> ColumnStats:
        ...

    @overload
    def get(self, column_name: str, /) -> ColumnStats:
        ...

    def get(self, column_or_column_name: Column | str) -> ColumnStats:
        """
        Returns the stats for the given column.

        Raises a `ValueError` if the given column is not in this table stats.
        """
        match column_or_column_name:
            case Column() as column:
                column_name = column.name
                if column in self._stats:
                    return self._stats[column]

            case str() as column_name:
                for column, column_stats in self._stats.items():
                    if column.name == column_name:
                        return column_stats

        raise ValueError(f"Column `{column_name}` does not exist")

    @staticmethod
    def from_dataframe(
        df: pd.DataFrame, table: Table, /, *, verbose: bool = False
    ) -> TableStats:
        """
        Compute the stats of the given table (i.e. all of the column stats)
        based on the given data.
        """
        with log_around(
            verbose,
            start=f"Computing the stats for table `{table.name}`",
            end=lambda: (
                f"Finished computing the stats for table `{table.name}` "
                f"in {measurement.total_seconds:.2f} seconds"
            ),
        ):
            with measure_time() as measurement:
                return TableStats(
                    {
                        column: ColumnStats.from_series(df[column.name], column)
                        for column in table.columns
                    }
                )

    @staticmethod
    def from_list(data: list[dict[str, Any]]) -> TableStats:
        """Converts the given JSON array into table stats."""
        return TableStats(
            dict(
                (
                    Column.from_dict(item["column"]),
                    ColumnStats.from_dict(item["column_stats"]),
                )
                for item in data
            )
        )

    def to_list(self) -> list[dict[str, Any]]:
        """Converts this table stats into a JSON array."""
        return [
            {
                "column": column.to_dict(),
                "column_stats": column_stats.to_dict(),
            }
            for column, column_stats in self._stats.items()
        ]


@final
@define
class DataStats:
    """Container for the statistics of all tables in a dataset."""

    _stats: Mapping[Table, TableStats]

    @overload
    def get(self, table: Table, /) -> TableStats:
        ...

    @overload
    def get(self, table_name: str, /) -> TableStats:
        ...

    def get(self, table_or_table_name: Table | str) -> TableStats:
        """
        Returns the stats for the given table.

        Raises a `ValueError` if the given table is not in this data stats.
        """
        match table_or_table_name:
            case Table() as table:
                table_name = table.name
                if table in self._stats:
                    return self._stats[table]

            case str() as table_name:
                for table, table_stats in self._stats.items():
                    if table.name == table_name:
                        return table_stats

        raise ValueError(f"Table `{table_name}` does not exist")

    @staticmethod
    def from_dataset(
        dataset: Dataset, /, *, concurrent: bool = False, verbose: bool = False
    ) -> DataStats:
        """
        Computes the stats of the given dataset.

        Raises an `ValueError` if any of the tables cannot be loaded,
        or if the table statistics cannot be computed.
        """
        with log_around(
            verbose,
            start=f"Computing the stats for dataset `{dataset.name}`\n---",
            end=lambda: (
                f"---\nFinished computing the stats for dataset `{dataset.name}` "
                f"in {measurement.total_seconds:.2f} seconds"
            ),
        ):
            with measure_time() as measurement:
                if concurrent:
                    with mp.Pool() as pool:
                        stats = pool.starmap(
                            DataStats._compute_table_stats,
                            zip(repeat(dataset), dataset.tables, repeat(verbose)),
                        )
                else:
                    stats = [
                        DataStats._compute_table_stats(dataset, table, verbose)
                        for table in dataset.tables
                    ]

        return DataStats(dict(zip(dataset.tables, stats)))

    @staticmethod
    def _compute_table_stats(
        dataset: Dataset, table: Table, verbose: bool
    ) -> TableStats:
        """
        Helper method for computing the stats of the given table.

        This is especially needed since `multiprocessing` can only execute
        functions in the global scope (i.e. not lambda or nested functions).
        """
        return TableStats.from_dataframe(
            dataset.get_dataframe(table), table, verbose=verbose
        )

    @classmethod
    def from_list(cls, data: list[dict[str, Any]]) -> DataStats:
        """Converts the given JSON array into data stats."""
        return DataStats(
            dict(
                (
                    Table.from_dict(item["table"]),
                    TableStats.from_list(item["table_stats"]),
                )
                for item in data
            )
        )

    def to_list(self) -> list[dict[str, Any]]:
        """Converts this data stats into a JSON array."""
        return [
            {"table": table.to_dict(), "table_stats": table_stats.to_list()}
            for table, table_stats in self._stats.items()
        ]

    @staticmethod
    def load(f: TextIO) -> DataStats:
        """
        Reads the given stream and loads it into a data stats instance.
        """
        return DataStats.from_list(json.load(f))

    def dump(self, f: TextIO) -> None:
        """
        Serializes this data stats instance and writes it into the given stream.
        """
        json.dump(self.to_list(), f)
