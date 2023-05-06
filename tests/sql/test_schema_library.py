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


@pytest.fixture(name="author_id")
def fixture_author_id() -> Column:
    return Column(
        name="id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_primary_key=True),
    )


@pytest.fixture(name="author_name")
def fixture_author_name() -> Column:
    return Column(
        name="name",
        dtype=DataType.STRING,
        constraint=ColumnConstraint(is_not_null=True, max_char_length=64),
    )


@pytest.fixture(name="author")
def fixture_author(author_id: Column, author_name: Column) -> Table:
    return Table(
        name="author",
        columns=[author_id, author_name],
    )


@pytest.fixture(name="book_id")
def fixture_book_id() -> Column:
    return Column(
        name="id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_primary_key=True),
    )


@pytest.fixture(name="book_title")
def fixture_book_title() -> Column:
    return Column(
        name="title",
        dtype=DataType.STRING,
        constraint=ColumnConstraint(
            is_unique=True,  # Assume books have unique titles
            is_not_null=False,  # Assume books can be untitled
            max_char_length=256,
        ),
    )


@pytest.fixture(name="book_price")
def fixture_book_price() -> Column:
    return Column(
        name="price",
        dtype=DataType.FLOAT,
    )


@pytest.fixture(name="book_author_id")
def fixture_book_author_id() -> Column:
    return Column(
        name="author_id",
        dtype=DataType.INTEGER,
        constraint=ColumnConstraint(is_foreign_key=True, is_not_null=True),
    )


@pytest.fixture(name="book")
def fixture_book(
    book_id: Column, book_title: Column, book_price: Column, book_author_id: Column
) -> Table:
    return Table(
        name="book",
        columns=[book_id, book_title, book_price, book_author_id],
    )


@pytest.fixture(name="library")
def fixture_library(
    book: Table, book_author_id: Column, author: Table, author_id: Column
) -> Schema:
    return Schema(
        tables=(tables := [author, book]),
        relationships=RelationshipGraph(
            tables=tables,
            relationships=[(book, book_author_id, author, author_id)],
        ),
    )


@pytest.fixture(name="library_dict")
def fixture_library_dict() -> dict[str, Any]:
    return {
        "tables": [
            {
                "name": "author",
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
                        "name": "name",
                        "dtype": "character varying",
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": True,
                        "max_char_length": 64,
                    },
                ],
            },
            {
                "name": "book",
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
                        "is_unique": True,
                        "is_not_null": False,
                        "max_char_length": 256,
                    },
                    {
                        "name": "price",
                        "dtype": "real",
                        "is_primary_key": False,
                        "is_foreign_key": False,
                        "is_unique": False,
                        "is_not_null": False,
                        "max_char_length": None,
                    },
                    {
                        "name": "author_id",
                        "dtype": "integer",
                        "is_primary_key": False,
                        "is_foreign_key": True,
                        "is_unique": False,
                        "is_not_null": True,
                        "max_char_length": None,
                    },
                ],
            },
        ],
        "relationships": [["book", "author_id", "author", "id"]],
    }


class TestSchema:
    def test_get_table_exists(
        self, library: Schema, author: Table, book: Table
    ) -> None:
        assert library.get_table("author") == author
        assert library.get_table("book") == book

    def test_get_table_not_exists(self, library: Schema) -> None:
        with pytest.raises(ValueError):
            library.get_table("publisher")

    def test_from_dict(self, library: Schema, library_dict: dict[str, Any]) -> None:
        assert Schema.from_dict(library_dict) == library

    def test_to_dict(self, library: Schema, library_dict: dict[str, Any]) -> None:
        assert library.to_dict() == library_dict

    def test_load(self, library: Schema, library_dict: dict[str, Any]) -> None:
        # pylint: disable-next=abstract-class-instantiated
        assert Schema.load(StringIO(json.dumps(library_dict))) == library

    def test_dump(self, library: Schema, library_dict: dict[str, Any]) -> None:
        buffer = StringIO()  # pylint: disable=abstract-class-instantiated
        library.dump(buffer)
        buffer.seek(0)

        assert json.load(buffer) == library_dict


class TestRelationshipGraph:
    @pytest.mark.parametrize(
        "table_name, column_name, possible_joins_str",
        [
            ("book", "id", set()),
            ("author", "id", {("book", "author_id")}),
        ],
    )
    def test_get_possible_joins(
        self,
        library: Schema,
        table_name: str,
        column_name: str,
        possible_joins_str: Set[tuple[str, str]],
    ) -> None:
        table = library.get_table(table_name)
        column = table.get_column(column_name)
        possible_joins = {
            TableColumn(
                (join_table := library.get_table(join_table_name)),
                join_table.get_column(join_column_name),
            )
            for join_table_name, join_column_name in possible_joins_str
        }

        assert library.relationships.get_possible_joins(table, column) == possible_joins

    def test_from_list(self, library: Schema, library_dict: dict[str, Any]) -> None:
        assert (
            RelationshipGraph.from_list(library_dict["relationships"], library.tables)
            == library.relationships
        )

    def test_to_list(self, library: Schema, library_dict: dict[str, Any]) -> None:
        def deorder(array: list[list[Any]]) -> set[tuple[Any, ...]]:
            return set(tuple(item) for item in array)

        actual = library.relationships.to_list()
        expected = library_dict["relationships"]

        assert len(actual) == len(expected)
        assert deorder(actual) == deorder(expected)


class TestTable:
    def test_table_columns(
        self,
        book: Table,
        book_id: Column,
        book_title: Column,
        book_price: Column,
        book_author_id: Column,
    ) -> None:
        assert book.table_columns == [
            TableColumn(book, book_id),
            TableColumn(book, book_title),
            TableColumn(book, book_price),
            TableColumn(book, book_author_id),
        ]

    def test_get_column_exists(
        self, author: Table, author_id: Column, author_name: Column
    ) -> None:
        assert author.get_column("id") == author_id
        assert author.get_column("name") == author_name

    def test_get_column_not_exists(self, book: Table) -> None:
        with pytest.raises(ValueError):
            book.get_column("illustrator")

    def test_from_dict(
        self, library_dict: dict[str, Any], author: Table, book: Table
    ) -> None:
        assert Table.from_dict(library_dict["tables"][0]) == author
        assert Table.from_dict(library_dict["tables"][1]) == book

    def test_to_dict(
        self, library_dict: dict[str, Any], author: Table, book: Table
    ) -> None:
        assert author.to_dict() == library_dict["tables"][0]
        assert book.to_dict() == library_dict["tables"][1]


class TestColumn:
    @pytest.mark.parametrize(
        "column_index, is_primary_key, is_foreign_key",
        [
            (0, True, False),
            (1, False, False),
            (2, False, False),
            (3, False, True),
        ],
    )
    def test_constraints(
        self, book: Table, column_index: int, is_primary_key: bool, is_foreign_key: bool
    ) -> None:
        assert book.columns[column_index].is_primary_key == is_primary_key
        assert book.columns[column_index].is_foreign_key == is_foreign_key

    def test_from_dict(
        self, library_dict: dict[str, Any], author_id: Column, author_name: Column
    ) -> None:
        author_dict = library_dict["tables"][0]
        assert Column.from_dict(author_dict["columns"][0]) == author_id
        assert Column.from_dict(author_dict["columns"][1]) == author_name

    def test_to_dict(
        self, library_dict: dict[str, Any], author_id: Column, author_name: Column
    ) -> None:
        author_dict = library_dict["tables"][0]
        assert author_id.to_dict() == author_dict["columns"][0]
        assert author_name.to_dict() == author_dict["columns"][1]


class TestDataType:
    @pytest.mark.parametrize(
        "arg, expected",
        [
            ("integer", DataType.INTEGER),
            ("pg_catalog.float4", DataType.FLOAT),
            ("character varying", DataType.STRING),
            ("pg_catalog.bool", DataType.BOOLEAN),
        ],
    )
    def test_valid_str(self, arg: str, expected: DataType) -> None:
        assert DataType.from_str(arg) == expected

    @pytest.mark.parametrize(
        "arg",
        ["int", "float", "text", "bool"],
    )
    def test_invalid_str(self, arg: str) -> None:
        with pytest.raises(ValueError):
            assert DataType.from_str(arg)
