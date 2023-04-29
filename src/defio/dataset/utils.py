from __future__ import annotations

import gzip
import shutil
from abc import abstractmethod
from collections.abc import Callable, Iterator, Sequence
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Final, Generic, Protocol, Self, TextIO, TypeVar, final

import attrs
from attrs import define

from defio.utils.attrs import to_tuple

NULL_SEQUENCE: Final = r"\N"  # Postgres' default null sequence (also Redshift)

_T = TypeVar("_T")


@final
class NullableFields(list[str]):
    """
    Helper class representing a list of nullable string fields
    of a tuple/record/row in a dataset.
    """

    def get(self, index: int) -> str | None:
        """
        Returns the field value at the given index or `None`
        if it is the null sequence.

        Raises an `IndexError` if the given index is out of bounds.
        """
        if not 0 <= index < len(self):
            raise IndexError("Index out of bounds")

        return field if not _is_field_null(field := self[index]) else None

    def require(self, index: int) -> str:
        """
        Returns the field value at the given index.

        Raises a `ValueError` if the value at the given index
        is the null sequence.

        Raises an `IndexError` if the given index is out of bounds.
        """
        if not 0 <= index < len(self):
            raise IndexError("Index out of bounds")

        if _is_field_null(field := self[index]):
            raise ValueError("Field is null")

        return field

    def map(self, index: int, func: Callable[[str], _T]) -> _T | None:
        """
        Returns the result of applying `func` to the field value
        at the given index, or `None` if the value is the null sequence.

        Raises an `IndexError` if the given index is out of bounds.
        """
        if not 0 <= index < len(self):
            raise IndexError("Index out of bounds")

        return func(field) if (field := self.get(index)) is not None else None


class TsvReadable(Protocol):
    """
    Protocol for classes that represent a single tuple/record/row
    of a dataset that can be read from TSV.
    """

    @classmethod
    @abstractmethod
    def from_tsv(cls, fields: NullableFields) -> Self:
        raise NotImplementedError


_R = TypeVar("_R", bound=TsvReadable)


@final
@define
class TsvReader(Generic[_R]):
    """
    Helper class for reading dataset tuples/records/rows from a TSV file.
    """

    _fp: TextIO = attrs.field(alias="fp")
    _close_later: bool = attrs.field(alias="close_later")
    target_class: type[_R]
    skip_header: bool

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Only close the file that we opened
        if self._close_later:
            self.close()

    @staticmethod
    def open(
        path_or_fp: Path | TextIO, /, *, target_class: type[_R], skip_header: bool
    ) -> TsvReader:
        """
        Opens a TSV reader for the specified file or already-opened text stream.
        """
        if isinstance(path_or_fp, Path):
            fp = open(path_or_fp, mode="r", encoding="utf-8")
            close_later = True
        else:
            fp = path_or_fp
            close_later = False

        return TsvReader(
            fp=fp,
            close_later=close_later,
            target_class=target_class,
            skip_header=skip_header,
        )

    def close(self) -> None:
        """Closes this TSV reader."""
        self._fp.close()

    def __iter__(self) -> Iterator[_R]:
        num_columns: int | None = None
        if self.skip_header:
            num_columns = len(self._fp.readline().strip().split("\t"))

        for line in self._fp:
            fields = NullableFields(line.strip().split("\t"))

            if num_columns is None:
                num_columns = len(fields)

            # Check for consistent number of columns based on the first row
            # (which could be either the header or the first data row)
            assert len(fields) == num_columns

            yield self.target_class.from_tsv(fields)


@final
@define
class TsvWriter:
    """
    Helper class for writing dataset tuples/records/rows (in the form of
    individual fields) into a TSV file.
    """

    _fp: TextIO = attrs.field(alias="fp")
    _close_later: bool = attrs.field(alias="close_later")
    with_index: bool
    header: Sequence[str] | None
    line_number: int = attrs.field(default=1, init=False)  # Use 1-based indexing

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Only close the file that we opened
        if self._close_later:
            self.close()

    @staticmethod
    def open(
        path_or_fp: Path | TextIO, /, *, with_index: bool, header: Sequence[str] | None
    ) -> TsvWriter:
        """
        Opens a TSV writer for the specified file or already-opened text stream.

        If `with_index` is `True`, the first column of each row (which is
        included in `header`, if provided) will automatically be filled
        with an integer "index" representing the row number starting from 1.

        Raises a `ValueError` if header is provided but empty.
        """
        if isinstance(path_or_fp, Path):
            fp = open(path_or_fp, mode="w+", encoding="utf-8")
            close_later = True
        else:
            fp = path_or_fp
            close_later = False

        # Write header in advance
        if header is not None:
            if len(header) == 0:
                raise ValueError("Header cannot be empty")

            fp.write("\t".join(header) + "\n")

        return TsvWriter(
            fp=fp,
            close_later=close_later,
            with_index=with_index,
            header=to_tuple(header),
        )

    def close(self) -> None:
        """Closes this TSV writer."""
        self._fp.close()

    def write_line(
        self, fields: Sequence[int | float | str | bool | Enum | None]
    ) -> None:
        """
        Writes a sequence of fields into a single TSV line.

        Raises a `ValueError` if the `fields` is empty or if the length of
        `fields` does not match the header (which include the index number,
        if applicable).
        """
        if self.header is not None:
            if len(fields) + int(self.with_index) != len(self.header):
                raise ValueError("The number of fields must match the number of header")
        else:
            if len(fields) == 0:
                raise ValueError("Fields must not be empty")

        if self.with_index:
            self._fp.write(str(self.line_number) + "\t")

        self._fp.write("\t".join(_to_nullable_field(field) for field in fields) + "\n")

        self.line_number += 1


def compress_to_gzip(source: Path, target_dir: Path) -> None:
    """
    Compresses the given source file and puts it into the target directory.

    Reference: https://docs.python.org/3/library/gzip.html
    """
    with open(source, "rb") as f_in:
        with gzip.open(target_dir / f"{source.name}.gz", mode="wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def _is_field_null(field: str) -> bool:
    return field == NULL_SEQUENCE


def _to_nullable_field(value: int | float | str | Enum | None) -> str:
    if value is None:
        return NULL_SEQUENCE

    if isinstance(value, Enum):
        # Use 1-based indexing
        return str(_get_enum_index(value, value.__class__) + 1)

    if isinstance(value, bool):
        # NOTE: Redshift is case-sensitive with boolean values, unlike Postgres
        return str(value).upper()

    return str(value)


_E = TypeVar("_E", bound=Enum)


def _get_enum_index(member: _E, enum_class: type[_E]) -> int:
    try:
        return next(i for i, enum in enumerate(enum_class) if enum is member)
    except StopIteration as exc:
        raise RuntimeError("Should not reach here") from exc
