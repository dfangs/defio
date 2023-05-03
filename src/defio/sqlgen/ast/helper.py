from collections.abc import Sequence
from typing import final

from attrs import define

from defio.sql.schema import Column, Table


@final
@define(frozen=True, eq=False, unsafe_hash=True)
class UniqueTable:
    """
    Wrapper class for `Table` that is useful for generating `AliasedTable`.
    Specifically, it does not implement value equality, so each instance
    of unique table corresponds to a single table in the `FROM` clause,
    regardless of whether the underlying tables are the same.

    This class is _not_ a subclass of `Table`, but it exposes the same
    interface as a `Table`, for convenience.

    Note:
    This class needs to be in its own file in order to avoid circular imports
    between the `from_clause` and `expression` modules.
    """

    table: Table

    @property
    def name(self) -> str:
        return self.table.name

    @property
    def columns(self) -> Sequence[Column]:
        return self.table.columns
