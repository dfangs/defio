from typing import Final

import pglast
import pytest

from htap.sql import parser
from htap.sql.schema import (
    Column,
    ColumnConstraint,
    DataType,
    RelationshipGraph,
    Schema,
    Table,
)

IMDB_SCHEMA: Final = """
DROP TABLE IF EXISTS "title";
CREATE TABLE title (
    id character varying(9) PRIMARY KEY,
    title_type_id integer NOT NULL REFERENCES title_type(id),
    primary_title character varying NOT NULL,
    original_title character varying NOT NULL,
    is_adult boolean NOT NULL,
    start_year integer,
    end_year integer,
    runtime_minutes integer
);

DROP TABLE IF EXISTS "genre";
CREATE TABLE genre (
    id integer PRIMARY KEY,
    name character varying(16) UNIQUE NOT NULL
);

DROP TABLE IF EXISTS "title_genre";
CREATE TABLE title_genre (
    title_id character varying(9) REFERENCES title(id),
    genre_id integer REFERENCES genre(id),
    CONSTRAINT title_genre_pkey PRIMARY KEY (title_id, genre_id)
);

DROP TABLE IF EXISTS "title_type";
CREATE TABLE title_type (
    id integer PRIMARY KEY,
    name character varying(16) UNIQUE NOT NULL
);

DROP TABLE IF EXISTS "rating";
CREATE TABLE rating (
    id integer PRIMARY KEY,
    title_id character varying(9) NOT NULL REFERENCES title(id),
    average_rating real NOT NULL,
    num_votes integer NOT NULL
);
"""


def test_schema_valid() -> None:
    title = Table(
        name="title",
        columns=[
            Column(
                name="id",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(is_primary_key=True, max_char_length=9),
            ),
            Column(
                name="title_type_id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            Column(
                name="primary_title",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            Column(
                name="original_title",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            Column(
                name="is_adult",
                dtype=DataType.BOOLEAN,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            Column(name="start_year", dtype=DataType.INTEGER),
            Column(name="end_year", dtype=DataType.INTEGER),
            Column(name="runtime_minutes", dtype=DataType.INTEGER),
        ],
    )
    genre = Table(
        name="genre",
        columns=[
            Column(
                name="id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_primary_key=True),
            ),
            Column(
                name="name",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(
                    is_unique=True, is_not_null=True, max_char_length=16
                ),
            ),
        ],
    )
    title_genre = Table(
        name="title_genre",
        columns=[
            Column(
                name="title_id",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(max_char_length=9),
            ),
            Column(name="genre_id", dtype=DataType.INTEGER),
        ],
    )
    title_type = Table(
        name="title_type",
        columns=[
            Column(
                name="id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_primary_key=True),
            ),
            Column(
                name="name",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(
                    is_unique=True, is_not_null=True, max_char_length=16
                ),
            ),
        ],
    )
    rating = Table(
        name="rating",
        columns=[
            Column(
                name="id",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_primary_key=True),
            ),
            Column(
                name="title_id",
                dtype=DataType.STRING,
                constraint=ColumnConstraint(is_not_null=True, max_char_length=9),
            ),
            Column(
                name="average_rating",
                dtype=DataType.FLOAT,
                constraint=ColumnConstraint(is_not_null=True),
            ),
            Column(
                name="num_votes",
                dtype=DataType.INTEGER,
                constraint=ColumnConstraint(is_not_null=True),
            ),
        ],
    )

    actual = parser.parse_schema(IMDB_SCHEMA, schema_name="imdb")
    expected = Schema(
        name="imdb",
        tables=(tables := [title, genre, title_genre, title_type, rating]),
        relationships=RelationshipGraph(
            tables=tables,
            relationships=[
                (
                    title,
                    title.get_column("title_type_id"),
                    title_type,
                    title_type.get_column("id"),
                ),
                (
                    title_genre,
                    title_genre.get_column("title_id"),
                    title,
                    title.get_column("id"),
                ),
                (
                    title_genre,
                    title_genre.get_column("genre_id"),
                    genre,
                    genre.get_column("id"),
                ),
                (
                    rating,
                    rating.get_column("title_id"),
                    title,
                    title.get_column("id"),
                ),
            ],
        ),
    )

    assert actual == expected


def test_schema_invalid() -> None:
    with pytest.raises(ValueError):
        parser.parse_schema("SELECT title FROM book;", schema_name="invalid")


def _test_parse_single_statement(sql: str) -> None:
    statements = parser.parse_sql(sql)
    assert len(statements) == 1

    actual = pglast.parse_sql(str(statements[0]))[0].stmt
    expected = pglast.parse_sql(sql)[0].stmt
    assert actual == expected


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(
            "SELECT 1;",
            id="without tables",
        ),
        pytest.param(
            "SELECT author FROM book;",
            id="minimum",
        ),
        pytest.param(
            "SELECT author, isbn FROM book;",
            id="two targets",
        ),
        pytest.param(
            "SELECT book.author, book.isbn FROM book;",
            id="qualified targets",
        ),
        pytest.param(
            "SELECT b.author, b.isbn FROM book AS b;",
            id="table alias",
        ),
        pytest.param(
            "SELECT book.title, author.name"
            " FROM book JOIN author ON book.author_id = author.id;",
            id="simple equijoin",
        ),
        pytest.param(
            "SELECT b.title, a.name"
            " FROM book AS b JOIN author AS a ON b.author_id = a.id",
            id="equijoin with alias",
        ),
        pytest.param(
            "SELECT b.title, a.name"
            " FROM book AS b LEFT OUTER JOIN author AS a ON b.author_id = a.id",
            id="outer join",
        ),
        pytest.param(
            "SELECT b.title, a.name, p.name"
            " FROM book AS b"
            " JOIN author AS a ON b.author_id = a.id"
            " JOIN publisher AS p ON b.publisher_id = p.id",
            id="tri-join",
        ),
        pytest.param(
            "SELECT b.title, a.name, s.status"
            " FROM book AS b RIGHT OUTER JOIN author AS a ON b.author_id = a.id, status AS s",
            id="mixed join",
        ),
        pytest.param(
            "SELECT book.title FROM book WHERE book.year >= 2000;",
            id="simple predicate",
        ),
        pytest.param(
            "SELECT book.title FROM book"
            " WHERE book.year >= 2000 AND book.title LIKE 'The%';",
            id="compound predicate",
        ),
        pytest.param(
            "SELECT title FROM book"
            " WHERE (year IS NULL OR year >= 2000) AND title LIKE 'The%';",
            id="multi-compound predicate",
        ),
        pytest.param(
            "SELECT a.name, b.title"
            " FROM book AS b JOIN author AS b ON b.author_id = a.id"
            " WHERE b.pages <= 100 AND b.rating IN (8, 9, 10);",
            id="join and filter",
        ),
        pytest.param(
            "SELECT a.name, b.title"
            " FROM book AS b, author AS b"
            " WHERE b.author_id = a.id AND NOT a.name LIKE 'Tim%';",
            id="join as filter",
        ),
        pytest.param(
            "SELECT year, avg(price) FROM book WHERE year BETWEEN 2000 AND 2010;",
            id="aggregate function",
        ),
    ],
)
def test_parse_select(sql: str) -> None:
    _test_parse_single_statement(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1 FROM book; SELECT 2 FROM book;",
        "SELECT author.name FROM author; SELECT book.title FROM book;",
        "SELECT 1;;; SELECT 2",
    ],
)
def test_parse_multiple_statements(sql: str) -> None:
    statements = parser.parse_sql(sql)

    actual = [pglast.parse_sql(str(statement))[0].stmt for statement in statements]
    expected = [statement.stmt for statement in pglast.parse_sql(sql)]

    assert actual == expected


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT FROM book;",
        "SELECT 1 FROM 1 WHERE 1",
        "FROM book SELECT title",
        "SELECT title FROMM book;",
    ],
)
def test_parse_invalid(sql: str) -> None:
    with pytest.raises(ValueError):
        parser.parse_sql(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) FROM book;",
        "INSERT INTO book VALUES ('untitled', 2000);",
    ],
)
def test_parse_not_implemented(sql: str) -> None:
    with pytest.raises(ValueError):
        parser.parse_sql(sql)
