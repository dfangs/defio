import json
from collections.abc import Set
from io import StringIO
from typing import Any

import pytest

from defio.sql.schema import (
    Column,
    ColumnConstraint,
    DataType,
    RelationshipGraph,
    Schema,
    Table,
    TableColumn,
)


@pytest.fixture(name="crew_id")
def fixture_crew_id() -> Column:
    return Column(
        name="id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_primary_key=True),
    )


@pytest.fixture(name="crew_salary")
def fixture_crew_salary() -> Column:
    return Column(
        name="salary",
        dtype=DataType.FLOAT,
        constraint=ColumnConstraint(is_not_null=True),
    )


@pytest.fixture(name="crew_manager_id")
def fixture_crew_manager_id() -> Column:
    return Column(
        name="manager_id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_foreign_key=True),
    )


@pytest.fixture(name="crew")
def fixture_crew(
    crew_id: Column, crew_salary: Column, crew_manager_id: Column
) -> Table:
    return Table(
        name="crew",
        columns=[crew_id, crew_salary, crew_manager_id],
    )


@pytest.fixture(name="movie_id")
def fixture_movie_id() -> Column:
    return Column(
        name="id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_primary_key=True),
    )


@pytest.fixture(name="movie_title")
def fixture_movie_title() -> Column:
    return Column(
        name="title",
        dtype=DataType.STRING,
        constraint=ColumnConstraint(is_not_null=True, max_char_length=256),
    )


@pytest.fixture(name="movie")
def fixture_movie(movie_id: Column, movie_title: Column) -> Table:
    return Table(
        name="movie",
        columns=[movie_id, movie_title],
    )


@pytest.fixture(name="director_id")
def fixture_director_id() -> Column:
    return Column(
        name="id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_primary_key=True, is_foreign_key=True),
    )


@pytest.fixture(name="director_name")
def fixture_director_name() -> Column:
    return Column(
        name="name",
        dtype=DataType.STRING,
        constraint=ColumnConstraint(is_not_null=True),
    )


@pytest.fixture(name="director_is_award_winning")
def fixture_director_is_award_winning() -> Column:
    return Column(
        name="is_award_winning",
        dtype=DataType.BOOLEAN,
        constraint=ColumnConstraint(is_not_null=True),
    )


@pytest.fixture(name="director")
def fixture_director(
    director_id: Column, director_name: Column, director_is_award_winning: Column
) -> Table:
    return Table(
        name="director",
        columns=[director_id, director_name, director_is_award_winning],
    )


@pytest.fixture(name="md_movie_id")
def fixture_md_movie_id() -> Column:
    return Column(
        name="movie_id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_foreign_key=True),
    )


@pytest.fixture(name="md_director_id")
def fixture_md_director_id() -> Column:
    return Column(
        name="director_id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_foreign_key=True),
    )


@pytest.fixture(name="movie_director")
def fixture_movie_director(md_movie_id: Column, md_director_id: Column) -> Table:
    return Table(
        name="movie_director",
        columns=[md_movie_id, md_director_id],
    )


@pytest.fixture(name="imdb")
def fixture_imdb(
    crew: Table,
    crew_id: Column,
    crew_manager_id: Column,
    movie: Table,
    movie_id: Column,
    director: Table,
    director_id: Column,
    movie_director: Table,
    md_movie_id: Column,
    md_director_id: Column,
) -> Schema:
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


class TestSchema:
    """
    This test suite uses the `imdb` (dummy) schema, which has a more complicated
    relationship graph than the `library` schema (e.g., self-cycle, many-to-many
    relationship).
    """

    def test_from_dict(self, imdb: Schema, imdb_dict: dict[str, Any]) -> None:
        assert Schema.from_dict(imdb_dict) == imdb

    def test_to_dict(self, imdb: Schema, imdb_dict: dict[str, Any]) -> None:
        assert imdb.to_dict() == imdb_dict

    def test_load(self, imdb: Schema, imdb_dict: dict[str, Any]) -> None:
        # pylint: disable-next=abstract-class-instantiated
        assert Schema.load(StringIO(json.dumps(imdb_dict))) == imdb

    def test_dump(self, imdb: Schema, imdb_dict: dict[str, Any]) -> None:
        buffer = StringIO()  # pylint: disable=abstract-class-instantiated
        imdb.dump(buffer)
        buffer.seek(0)

        assert json.load(buffer) == imdb_dict


class TestRelationshipGraph:
    @pytest.mark.parametrize(
        "table_name, column_name, possible_joins_str",
        [
            ("crew", "salary", set()),
            ("crew", "manager_id", {("crew", "id")}),
            ("director", "id", {("crew", "id"), ("movie_director", "director_id")}),
            ("movie", "id", {("movie_director", "movie_id")}),
        ],
    )
    def test_get_possible_joins(
        self,
        imdb: Schema,
        table_name: str,
        column_name: str,
        possible_joins_str: Set[tuple[str, str]],
    ) -> None:
        table = imdb.get_table(table_name)
        column = table.get_column(column_name)
        possible_joins = {
            TableColumn(
                (join_table := imdb.get_table(join_table_name)),
                join_table.get_column(join_column_name),
            )
            for join_table_name, join_column_name in possible_joins_str
        }

        assert imdb.relationships.get_possible_joins(table, column) == possible_joins

    def test_from_list(self, imdb: Schema, imdb_dict: dict[str, Any]) -> None:
        assert (
            RelationshipGraph.from_list(imdb_dict["relationships"], imdb.tables)
            == imdb.relationships
        )

    def test_to_list(self, imdb: Schema, imdb_dict: dict[str, Any]) -> None:
        def deorder(array: list[list[Any]]) -> set[tuple[Any]]:
            return set(tuple(item) for item in array)

        actual = imdb.relationships.to_list()
        expected = imdb_dict["relationships"]

        assert len(actual) == len(expected)
        assert deorder(actual) == deorder(expected)
