from typing import Any

import pytest

from defio.sql.schema import (
    Column,
    ColumnConstraint,
    DataType,
    RelationshipGraph,
    Schema,
    Table,
)


# The below code introduces a `dataset` mark to pytest
# It is used to determine whether to run unit tests that require some dataset
# Reference: https://docs.pytest.org/en/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
def pytest_addoption(parser):
    parser.addoption(
        "--with-dataset",
        action="store_true",
        default=False,
        help="run tests that require some dataset",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "dataset: mark test as requiring some dataset to run"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--with-dataset"):
        # --with-dataset given in cli: do not skip slow tests
        return

    skip_dataset = pytest.mark.skip(reason="need --with-dataset option to run")
    for item in items:
        if "dataset" in item.keywords:
            item.add_marker(skip_dataset)


## Shared fixtures


@pytest.fixture(name="imdb_schema", scope="module")
def fixture_imdb_schema() -> Schema:
    # NOTE:
    # This fixture is shared among `sql` and `sqlgen` subpackages
    # Scope has to be `module` in order to allow chaining with function-scoped fixtures
    crew = Table(
        name="crew",
        columns=[
            crew_id := Column(
                name="id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_primary_key=True),
            ),
            Column(
                name="salary",
                dtype=DataType.FLOAT,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            crew_manager_id := Column(
                name="manager_id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_foreign_key=True),
            ),
        ],
    )

    movie = Table(
        name="movie",
        columns=[
            movie_id := Column(
                name="id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_primary_key=True),
            ),
            Column(
                name="title",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(is_not_null=True, max_char_length=256),
            ),
        ],
    )

    director = Table(
        name="director",
        columns=[
            director_id := Column(
                name="id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_primary_key=True, is_foreign_key=True),
            ),
            Column(
                name="name",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            Column(
                name="is_award_winning",
                dtype=DataType.BOOLEAN,
                constraint=ColumnConstraint(is_not_null=True),
            ),
        ],
    )

    movie_director = Table(
        name="movie_director",
        columns=[
            md_movie_id := Column(
                name="movie_id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_foreign_key=True),
            ),
            md_director_id := Column(
                name="director_id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_foreign_key=True),
            ),
        ],
    )

    return Schema(
        tables=(tables := [crew, movie, director, movie_director]),
        relationships=RelationshipGraph(
            tables=tables,
            relationships=[
                (crew, crew_manager_id, crew, crew_id),
                (director, director_id, crew, crew_id),
                (movie_director, md_movie_id, movie, movie_id),
                (movie_director, md_director_id, director, director_id),
            ],
        ),
    )


@pytest.fixture(name="imdb_dict")
def fixture_imdb_dict() -> dict[str, Any]:
    return {
        "tables": [
            {
                "name": "crew",
                "columns": [
                    {
                        "name": "id",
                        "dtype": "integer",
                        "is_primary_key": True,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                    {
                        "name": "salary",
                        "dtype": "real",
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": True,
                        "max_char_length": None,
                    },
                    {
                        "name": "manager_id",
                        "dtype": "integer",
                        "is_primary_key": False,
                        "is_foreign_key": True,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                ],
            },
            {
                "name": "movie",
                "columns": [
                    {
                        "name": "id",
                        "dtype": "integer",
                        "is_primary_key": True,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                    {
                        "name": "title",
                        "dtype": "character varying",
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": True,
                        "max_char_length": 256,
                    },
                ],
            },
            {
                "name": "director",
                "columns": [
                    {
                        "name": "id",
                        "dtype": "integer",
                        "is_primary_key": True,
                        "is_foreign_key": True,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                    {
                        "name": "name",
                        "dtype": "character varying",
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": True,
                        "max_char_length": None,
                    },
                    {
                        "name": "is_award_winning",
                        "dtype": "boolean",
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": True,
                        "max_char_length": None,
                    },
                ],
            },
            {
                "name": "movie_director",
                "columns": [
                    {
                        "name": "movie_id",
                        "dtype": "integer",
                        "is_primary_key": False,
                        "is_foreign_key": True,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                    {
                        "name": "director_id",
                        "dtype": "integer",
                        "is_primary_key": False,
                        "is_foreign_key": True,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                ],
            },
        ],
        "relationships": [
            ["crew", "manager_id", "crew", "id"],
            ["director", "id", "crew", "id"],
            ["movie_director", "director_id", "director", "id"],
            ["movie_director", "movie_id", "movie", "id"],
        ],
    }
