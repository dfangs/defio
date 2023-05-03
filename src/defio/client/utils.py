from collections.abc import Sequence
from typing import assert_never

from defio.dataset import Dataset
from defio.sql.schema import Table


def get_tables_to_load(
    dataset: Dataset, tables_to_load: Sequence[Table | str] | None = None
) -> Sequence[Table]:
    """
    Helper function that returns the list of tables to load from S3
    based on the value of `tables_to_load`.
    """

    def resolve_table(table_or_table_name: Table | str) -> Table:
        match table_or_table_name:
            case Table() as table:
                if table not in dataset.schema.tables:
                    raise ValueError(f"Table {table.name} is not in this dataset")
                return table

            case str() as table_name:
                return dataset.schema.get_table(table_name)

            case _:
                assert_never(table_or_table_name)

    # If no override, load all tables
    if tables_to_load is None:
        return dataset.tables

    return [
        resolve_table(table_or_table_name) for table_or_table_name in tables_to_load
    ]
