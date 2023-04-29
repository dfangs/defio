from collections.abc import Set
from functools import cached_property
from pathlib import Path
from typing import Literal, TypeAlias, final

import pandas as pd
from attrs import define

from htap.sql.parser import parse_schema
from htap.sql.schema import Schema, Table

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
    schema_path: Path
    tables_dir_path: Path
    load_config: DatasetLoadConfig

    @cached_property
    def schema(self) -> Schema:
        """
        Returns the corresponding schema of this dataset.

        Note that this property is computed lazily on first access
        (i.e. the schema file is not read on construction time)
        and cached for subsequent accesses.

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

    @cached_property
    def tables(self) -> Set[Table]:
        """Returns the set of tables in this dataset."""
        return frozenset(self.schema.tables)

    def _table_path(self, table: Table) -> Path:
        for path in self.tables_dir_path.iterdir():
            # This allows for both uncompressed and compressed files
            table_name = path.name.partition(".")[0]
            if table.name == table_name:
                return path

        raise ValueError(f"File for table `{table.name}` does not exist")

    def get_dataframe(self, table: Table | str) -> pd.DataFrame:
        """
        Loads the given table from the filesystem and returns the pandas dataframe.

        Raises a `ValueError` if the given table does not belong to this dataset,
        or if the corresponding file does not exist.
        """
        match table:
            case Table():
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
