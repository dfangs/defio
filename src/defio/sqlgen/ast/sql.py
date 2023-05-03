from abc import ABC, abstractmethod

from attrs import define

from defio.sql.ast import SQL


@define(frozen=True)
class GenSQL(ABC):
    """Abstract base class wrapper for `SQL`."""

    @abstractmethod
    def to_sql(self) -> SQL:
        """Convert this object into its corresponding `SQL` object."""
        raise NotImplementedError
