import gzip
import shutil
from abc import abstractmethod
from collections.abc import Callable, Iterator, Sequence
from enum import Enum
from pathlib import Path
from typing import Final, Generic, Protocol, Self, TextIO, TypeVar, final

import attrs
from attrs import define

_T = TypeVar("_T")

NULL_SEQUENCE_SOURCE: Final = r"\N"  # Single backslash; used in the source context
NULL_SEQUENCE_TARGET: Final = r"\\N"  # Double backslash; used in the target context


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

    f: TextIO
    target_class: type[_R] = attrs.field(kw_only=True)
    skip_header: bool = attrs.field(kw_only=True)
    _num_columns: int | None = attrs.field(default=None, init=False)

    def __attrs_post_init__(self) -> None:
        if self.skip_header:
            self._num_columns = len(self.f.readline().strip().split("\t"))

    def __iter__(self) -> Iterator[_R]:
        for line in self.f:
            fields = NullableFields(line.strip().split("\t"))

            if self._num_columns is None:
                self._num_columns = len(fields)

            # Check for consistent number of columns based on the first row
            # (which could be either the headers or the first data row)
            assert len(fields) == self._num_columns

            yield self.target_class.from_tsv(fields)


@final
@define
class TsvWriter:
    """
    Helper class for writing dataset tuples/records/rows (in the form of
    individual fields) into a TSV file.
    """

    f: TextIO
    with_index: bool = attrs.field(kw_only=True)
    headers: Sequence[str] | None = attrs.field(default=None, kw_only=True)
    line_number: int = attrs.field(default=1, init=False)  # Use 1-based indexing

    def __attrs_post_init__(self) -> None:
        if self.headers is not None:
            assert len(self.headers) > 0
            self.f.write("\t".join(self.headers) + "\n")

    def write_line(
        self, fields: Sequence[int | float | str | bool | Enum | None]
    ) -> None:
        """
        Writes a sequence of fields into a single TSV line.

        Raises a `ValueError` if the `fields` is empty or
        if the length of `fields` does not match the headers
        (which include the index number, if applicable).
        """
        if self.headers is not None:
            if len(fields) + int(self.with_index) != len(self.headers):
                raise ValueError(
                    "The number of fields must match the number of headers"
                )
        else:
            if len(fields) == 0:
                raise ValueError("Fields must not be empty")

        if self.with_index:
            self.f.write(str(self.line_number) + "\t")

        self.f.write("\t".join(_to_nullable_field(field) for field in fields) + "\n")

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
    return field == NULL_SEQUENCE_SOURCE


def _to_nullable_field(value: int | float | str | Enum | None) -> str:
    if value is None:
        return NULL_SEQUENCE_TARGET

    if isinstance(value, Enum):
        # Use 1-based indexing
        return str(_get_enum_index(value, value.__class__) + 1)

    return str(value)


_E = TypeVar("_E", bound=Enum)


def _get_enum_index(member: _E, enum_class: type[_E]) -> int:
    try:
        return next(i for i, enum in enumerate(enum_class) if enum is member)
    except StopIteration as exc:
        raise RuntimeError("Should not reach here") from exc
