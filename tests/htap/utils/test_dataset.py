from __future__ import annotations

import gzip
import tempfile
from collections.abc import Callable, Sequence
from enum import Enum
from pathlib import Path
from typing import Any, final

import asyncstdlib as a
import pytest
from attrs import define
from typing_extensions import override

from htap.utils.dataset import (
    NULL_SEQUENCE_SOURCE,
    NULL_SEQUENCE_TARGET,
    NullableFields,
    TsvReadable,
    TsvReader,
    TsvWriter,
    compress_to_gzip,
)


class TestNullableFields:
    @pytest.fixture(name="fields")
    def fixture_fields(self) -> NullableFields:
        return NullableFields(
            [
                "untitled",
                NULL_SEQUENCE_SOURCE,
                "1",
                "a,b,c",
            ]
        )

    @pytest.mark.parametrize(
        "index, expected",
        [
            (0, "untitled"),
            (1, None),
            (2, "1"),
            (3, "a,b,c"),
        ],
    )
    def test_get_valid(
        self, fields: NullableFields, index: int, expected: str | None
    ) -> None:
        assert fields.get(index) == expected

    @pytest.mark.parametrize(
        "index",
        [-1, 4, 999_999_999],
    )
    def test_get_invalid(self, fields: NullableFields, index: int) -> None:
        with pytest.raises(IndexError):
            assert fields.get(index)

    @pytest.mark.parametrize(
        "index, expected",
        [
            (0, "untitled"),
            (2, "1"),
            (3, "a,b,c"),
        ],
    )
    def test_require_valid(
        self, fields: NullableFields, index: int, expected: str
    ) -> None:
        assert fields.require(index) == expected

    @pytest.mark.parametrize(
        "index, exception",
        [(1, ValueError), (-1, IndexError), (5, IndexError)],
    )
    def test_require_invalid(
        self, fields: NullableFields, index: int, exception: type[Exception]
    ) -> None:
        with pytest.raises(exception):
            fields.require(index)

    @pytest.mark.parametrize(
        "index, expected, func",
        [
            (0, "untitled", lambda f: f),
            (1, None, bool),
            (2, 1, int),
            (3, ["a", "b", "c"], lambda f: f.split(",")),
        ],
    )
    def test_map_valid(
        self,
        fields: NullableFields,
        index: int,
        expected: Any,
        func: Callable[[str], Any],
    ) -> None:
        assert fields.map(index, func) == expected

    @pytest.mark.parametrize(
        "index",
        [-1, 4, 999_999_999],
    )
    def test_map_invalid(self, fields: NullableFields, index: int) -> None:
        with pytest.raises(IndexError):
            fields.map(index, lambda f: f)


@final
@define(frozen=True)
class DummyTsvReadable(TsvReadable):
    id: int
    name: str

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> DummyTsvReadable:
        return DummyTsvReadable(int(fields.require(0)), fields.require(1))


class TestTsvReader:
    @pytest.fixture(name="rows")
    def fixture_rows(self) -> Sequence[DummyTsvReadable]:
        return [
            DummyTsvReadable(0, "tim"),
            DummyTsvReadable(1, "mit"),
            DummyTsvReadable(2, "mim"),
        ]

    @pytest.mark.asyncio
    async def test_skip_header(self, rows: Sequence[DummyTsvReadable]) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_file = Path(tmpdirname) / "test"

            with open(test_file, mode="w+", encoding="utf-8") as f:
                f.write(
                    "id\tname\n" + "".join([f"{row.id}\t{row.name}\n" for row in rows])
                )

            async with await TsvReader.open(
                test_file, target_class=DummyTsvReadable, skip_header=True
            ) as reader:
                async for i, row in a.enumerate(reader):
                    assert row == rows[i]

    @pytest.mark.asyncio
    async def test_not_skip_header(self, rows: Sequence[DummyTsvReadable]) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_file = Path(tmpdirname) / "test"

            with open(test_file, mode="w+", encoding="utf-8") as f:
                f.write("".join([f"{row.id}\t{row.name}\n" for row in rows]))

            async with await TsvReader.open(
                test_file, target_class=DummyTsvReadable, skip_header=False
            ) as reader:
                async for i, row in a.enumerate(reader):
                    assert row == rows[i]


class DummyEnum(Enum):
    ZERO = 0
    ONE = 1


class TestTsvWriter:
    @pytest.mark.parametrize(
        "fields, expected",
        [
            pytest.param([0, 1], "0\t1\n", id="int"),
            pytest.param([0.1, 1.2], "0.1\t1.2\n", id="float"),
            pytest.param(["a", "b"], "a\tb\n", id="str"),
            pytest.param([True, False], "True\tFalse\n", id="bool"),
            pytest.param([DummyEnum.ZERO, DummyEnum.ONE], "1\t2\n", id="Enum"),
            pytest.param(
                [None, None],
                f"{NULL_SEQUENCE_TARGET}\t{NULL_SEQUENCE_TARGET}\n",
                id="None",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_plain_valid(
        self, fields: Sequence[int | float | str | bool | Enum | None], expected: str
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_file = Path(tmpdirname) / "test"

            async with await TsvWriter.open(
                test_file, with_index=False, header=None
            ) as writer:
                await writer.write_line(fields)

            with open(test_file, mode="r", encoding="utf-8") as f:
                assert f.read() == expected

    @pytest.mark.asyncio
    async def test_plain_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            async with await TsvWriter.open(
                Path(tmpdirname) / "test", with_index=False, header=None
            ) as writer:
                with pytest.raises(ValueError):
                    await writer.write_line([])

    @pytest.mark.asyncio
    async def test_with_index_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_file = Path(tmpdirname) / "test"
            expected = ""

            async with await TsvWriter.open(
                test_file, with_index=True, header=None
            ) as writer:
                for i in range(5):
                    await writer.write_line([i, writer.line_number])
                    expected += f"{i + 1}\t{i}\t{i + 1}\n"

            with open(test_file, mode="r", encoding="utf-8") as f:
                assert f.read() == expected

    @pytest.mark.asyncio
    async def test_with_index_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            async with await TsvWriter.open(
                Path(tmpdirname) / "test", with_index=True, header=None
            ) as writer:
                with pytest.raises(ValueError):
                    await writer.write_line([])

    @pytest.mark.asyncio
    async def test_with_headers_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_file = Path(tmpdirname) / "test"

            async with await TsvWriter.open(
                test_file, with_index=False, header=["name", "age"]
            ) as writer:
                await writer.write_line(["tim", 23])

            with open(test_file, mode="r", encoding="utf-8") as f:
                assert f.read() == "name\tage\n" + "tim\t23\n"

    @pytest.mark.parametrize(
        "fields",
        [[], ["tim"], ["tim", 23, "mit"]],
    )
    @pytest.mark.asyncio
    async def test_with_headers_invalid(
        self, fields: Sequence[int | float | str | bool | Enum | None]
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            async with await TsvWriter.open(
                Path(tmpdirname) / "test", with_index=False, header=["name", "age"]
            ) as writer:
                with pytest.raises(ValueError):
                    await writer.write_line(fields)

    @pytest.mark.asyncio
    async def test_with_index_and_headers_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            test_file = Path(tmpdirname) / "test"
            expected = "id\tname\tage\n"

            async with await TsvWriter.open(
                test_file, with_index=True, header=["id", "name", "age"]
            ) as writer:
                for i in range(5):
                    await writer.write_line([f"tim_{i}", writer.line_number])
                    expected += f"{i + 1}\ttim_{i}\t{i + 1}\n"

            with open(test_file, mode="r", encoding="utf-8") as f:
                assert f.read() == expected

    @pytest.mark.parametrize(
        "fields",
        [[], ["tim"], [1, "tim", 23]],
    )
    @pytest.mark.asyncio
    async def test_with_index_and_headers_invalid(
        self, fields: Sequence[int | float | str | bool | Enum | None]
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            async with await TsvWriter.open(
                Path(tmpdirname) / "test",
                with_index=True,
                header=["id", "name", "age"],
            ) as writer:
                with pytest.raises(ValueError):
                    await writer.write_line(fields)


def test_gzip() -> None:
    with tempfile.TemporaryDirectory() as tmpdirname:
        test_dir = Path(tmpdirname)
        source_path = test_dir / "test.tsv"
        target_path = test_dir / "test.tsv.gz"

        test_string = "1\t2\t3\n"
        with open(source_path, mode="w+", encoding="utf-8") as f:
            f.write(test_string)

        compress_to_gzip(source_path, test_dir)

        assert target_path.exists()
        with gzip.open(target_path, mode="rt", encoding="utf-8") as f:
            assert f.read() == test_string
