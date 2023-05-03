import csv
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from typing import Final

from defio.dataset.dataset import Dataset, DatasetLoadConfig
from defio.dataset.stats import DataStats

COMPANY_SCHEMA: Final = """
DROP TABLE IF EXISTS employee;
CREATE TABLE employee (
    id integer PRIMARY KEY,
    name character varying(64) NOT NULL,
    salary real NOT NULL,
    dept_id integer NOT NULL REFERENCES dept(id)
);

DROP TABLE IF EXISTS dept;
CREATE TABLE dept (
    id integer PRIMARY KEY,
    name character varying(32) NOT NULL,
    is_critical boolean NOT NULL
);
"""


@contextmanager
def create_company_dataset() -> Iterator[Dataset]:
    def to_tsv_line(*values: str) -> str:
        return "\t".join(values) + "\n"

    with tempfile.TemporaryDirectory() as tmpdirname:
        dataset_path = Path(tmpdirname)
        schema_filename = "schema.sql"
        stats_filename = "stats.json"
        tables_dirname = "tsv"
        schema_path = dataset_path / schema_filename
        tables_path = dataset_path / tables_dirname

        with open(schema_path, mode="w+", encoding="utf-8") as f:
            f.write(COMPANY_SCHEMA)

        tables_path.mkdir(parents=True, exist_ok=True)

        with open(tables_path / "employee.tsv", mode="w+", encoding="utf-8") as f:
            f.write(to_tsv_line("id", "name", "salary", "dept_id"))
            f.write(to_tsv_line("1", "Alice", "123.50", "1"))
            f.write(to_tsv_line("2", "Bob", "111.11", "1"))
            f.write(to_tsv_line("3", "Charlie", "99", "2"))

        with open(tables_path / "dept.tsv", mode="w+", encoding="utf-8") as f:
            f.write(to_tsv_line("id", "name", "is_critical"))
            f.write(to_tsv_line("1", "Engineering", "True"))
            f.write(to_tsv_line("2", "Hiking", "False"))

        yield Dataset(
            name="company",
            directory=dataset_path,
            schema_filename=schema_filename,
            stats_filename=stats_filename,
            tables_dirname=tables_dirname,
            load_config=DatasetLoadConfig(
                delimiter="\t",
                skip_header=True,
                na_value=r"\N",
                quoting=csv.QUOTE_NONE,
                escape_char=None,
            ),
        )


def test_getters() -> None:
    with create_company_dataset() as dataset:
        data_stats = DataStats.from_dataset(dataset, concurrent=True, verbose=True)

        # Only test that these do not raise any exception
        # (testing for expected values is too tedious)
        for table in dataset.tables:
            table_stats = data_stats.get(table)

            for column in table.columns:
                table_stats.get(column)


def test_json_conversion() -> None:
    with create_company_dataset() as dataset:
        stats = DataStats.from_dataset(dataset, concurrent=False, verbose=False)
        serde_stats = DataStats.from_list(stats.to_list())

        # Can only compare via JSON since `DataStats` doesn't implement value equality
        assert serde_stats.to_list() == stats.to_list()


def test_load_and_dump() -> None:
    with create_company_dataset() as dataset:
        stats = DataStats.from_dataset(dataset, concurrent=False, verbose=False)

        stream = StringIO()
        stats.dump(stream)

        stream.seek(0)
        serde_stats = stats.load(stream)

        # Can only compare via JSON since `DataStats` doesn't implement value equality
        assert serde_stats.to_list() == stats.to_list()


def test_dataset_stats() -> None:
    with create_company_dataset() as dataset:
        stats = DataStats.from_dataset(dataset, concurrent=False, verbose=False)

        # Write to a file instead of `StringIO`
        with open(dataset.stats_path, mode="w+", encoding="utf-8") as f:
            stats.dump(f)

        # `Dataset.stats` property loads the stats from file
        serde_stats = dataset.stats

        # Can only compare via JSON since `DataStats` doesn't implement value equality
        assert serde_stats.to_list() == stats.to_list()
