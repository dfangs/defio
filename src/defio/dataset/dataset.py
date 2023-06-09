from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import Literal, TypeAlias, final, overload

import pandas as pd
from attrs import define

from defio.dataset.stats import DataStats
from defio.sql.parser import parse_schema
from defio.sql.schema import Schema, Table

_CSVQuoting: TypeAlias = Literal[0, 1, 2, 3]


@final
@define(frozen=True, kw_only=True)
class DatasetLoadConfig:
    """
    Configuration for loading a dataset via `pandas.read_csv()`.
    """

    delimiter: str
    skip_header: bool
    na_value: str
    quoting: _CSVQuoting
    escape_char: str | None


# Cannot use `slots=True` with `@cached_property`
@final
@define(frozen=True, kw_only=True, slots=False)
class Dataset:
    """
    Represents a dataset residing in a filesystem.

    This class contains all the information required to load the dataset
    from the filesystem (e.g., path to schema file, table files).
    """

    name: str
    directory: Path
    schema_filename: str
    stats_filename: str
    tables_dirname: str
    load_config: DatasetLoadConfig

    @property
    def schema_path(self) -> Path:
        """Returns the path to this dataset's schema file."""
        return self.directory / self.schema_filename

    @property
    def stats_path(self) -> Path:
        """Returns the path to this dataset's stats file."""
        return self.directory / self.stats_filename

    @property
    def tables_dirpath(self) -> Path:
        """Returns the path to the directory containing this dataset's tables."""
        return self.directory / self.tables_dirname

    @cached_property
    def schema(self) -> Schema:
        """
        Returns the corresponding schema of this dataset.

        Note that this property is computed lazily on first access
        (i.e. the schema file is not read on construction time)
        and cached for subsequent access.

        Raises a `ValueError` if the schema cannot be read from the filesystem,
        or if the file cannot be parsed as a valid schema.
        """
        try:
            with open(self.schema_path, mode="r", encoding="utf-8") as f:
                return parse_schema(f.read())

        except OSError as exc:
            raise ValueError("Schema file does not exist") from exc

        except ValueError as exc:
            raise ValueError("Schema cannot be parsed") from exc

    @property
    def tables(self) -> Sequence[Table]:
        """Returns the set of tables in this dataset."""
        return self.schema.tables

    @cached_property
    def stats(self) -> DataStats:
        """
        Returns the corresponding stats of this dataset.

        Note that this property does not compute the stats on-demand;
        it only loads the precomputed stats from the given file and
        caches the result for subsequent access.

        Raises a `ValueError` if the stats cannot be read from the filesystem,
        or if the file cannot be parsed as a valid `DataStats`.
        """
        try:
            with open(self.stats_path, mode="r", encoding="utf-8") as f:
                return DataStats.load(f)

        except OSError as exc:
            raise ValueError("Stats file does not exist") from exc

        except ValueError as exc:
            raise ValueError("Stats cannot be parsed") from exc

    def _table_path(self, table: Table) -> Path:
        for path in self.tables_dirpath.iterdir():
            # This allows for both uncompressed and compressed files
            table_name = path.name.partition(".")[0]
            if table.name == table_name:
                return path

        raise ValueError(f"File for table `{table.name}` does not exist")

    @overload
    def get_dataframe(self, table: Table, /) -> pd.DataFrame:
        ...

    @overload
    def get_dataframe(self, table_name: str, /) -> pd.DataFrame:
        ...

    def get_dataframe(self, table_or_table_name: Table | str) -> pd.DataFrame:
        """
        Loads the given table from the filesystem and returns the pandas dataframe.

        Raises a `ValueError` if the given table does not belong to this dataset,
        or if the corresponding file does not exist.
        """
        match table_or_table_name:
            case Table() as table:
                if table not in self.tables:
                    raise ValueError(f"Table `{table.name}` is not in this dataset")

            case str() as table_name:
                table = self.schema.get_table(table_name)

        return pd.read_csv(
            self._table_path(table),
            delimiter=self.load_config.delimiter,
            names=(
                [column.name for column in table.columns]
                if self.load_config.skip_header
                else None
            ),
            header=0,  # Always expect the file to contain a header
            dtype={column.name: column.dtype.pandas_dtype for column in table.columns},
            na_values=self.load_config.na_value,
            keep_default_na=False,  # Only use the provided NA value
            quoting=self.load_config.quoting,
            escapechar=self.load_config.escape_char,
        )
