import json
from collections.abc import Set
from io import StringIO
from typing import Any

import pytest

from defio.sql.schema import RelationshipGraph, Schema, TableColumn


class TestSchema:
    """
    This test suite uses the `imdb` (dummy) schema, which has a more complicated
    relationship graph than the `library` schema (e.g., self-cycle, many-to-many
    relationship).
    """

    def test_from_dict(self, imdb_schema: Schema, imdb_dict: dict[str, Any]) -> None:
        assert Schema.from_dict(imdb_dict) == imdb_schema

    def test_to_dict(self, imdb_schema: Schema, imdb_dict: dict[str, Any]) -> None:
        assert imdb_schema.to_dict() == imdb_dict

    def test_load(self, imdb_schema: Schema, imdb_dict: dict[str, Any]) -> None:
        # pylint: disable-next=abstract-class-instantiated
        assert Schema.load(StringIO(json.dumps(imdb_dict))) == imdb_schema

    def test_dump(self, imdb_schema: Schema, imdb_dict: dict[str, Any]) -> None:
        buffer = StringIO()  # pylint: disable=abstract-class-instantiated
        imdb_schema.dump(buffer)
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
        imdb_schema: Schema,
        table_name: str,
        column_name: str,
        possible_joins_str: Set[tuple[str, str]],
    ) -> None:
        table = imdb_schema.get_table(table_name)
        column = table.get_column(column_name)
        possible_joins = {
            TableColumn(
                (join_table := imdb_schema.get_table(join_table_name)),
                join_table.get_column(join_column_name),
            )
            for join_table_name, join_column_name in possible_joins_str
        }

        assert (
            imdb_schema.relationships.get_possible_joins(table, column)
            == possible_joins
        )

    def test_from_list(self, imdb_schema: Schema, imdb_dict: dict[str, Any]) -> None:
        assert (
            RelationshipGraph.from_list(imdb_dict["relationships"], imdb_schema.tables)
            == imdb_schema.relationships
        )

    def test_to_list(self, imdb_schema: Schema, imdb_dict: dict[str, Any]) -> None:
        def deorder(array: list[list[Any]]) -> set[tuple[Any, ...]]:
            return set(tuple(item) for item in array)

        actual = imdb_schema.relationships.to_list()
        expected = imdb_dict["relationships"]

        assert len(actual) == len(expected)
        assert deorder(actual) == deorder(expected)
