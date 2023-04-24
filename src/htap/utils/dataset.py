import gzip
import shutil
from collections.abc import Callable, Sequence
from enum import Enum
from pathlib import Path
from typing import Final, TextIO, TypeVar, final

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


@final
@define
class TsvWriter:
    """
    Helper class for writing dataset tuples/records/rows (in the form of
    individual fields) into TSV lines.
    """

    f: TextIO
    with_index: bool = attrs.field(kw_only=True)
    headers: Sequence[str] | None = attrs.field(default=None, kw_only=True)
    line_count: int = attrs.field(default=0, init=False)

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
        if the length of `fields` does not match the headers.
        """
        if self.headers is not None:
            if len(fields) != len(self.headers):
                raise ValueError(
                    "The number of fields must match the number of headers"
                )
        else:
            if len(fields) == 0:
                raise ValueError("Fields must not be empty")

        self.line_count += 1  # Use 1-based indexing

        if self.with_index:
            self.f.write(str(self.line_count) + "\t")

        self.f.write("\t".join(_to_nullable_field(field) for field in fields) + "\n")


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
