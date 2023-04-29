from abc import ABC, abstractmethod

from attrs import define


@define(frozen=True)
class SQL(ABC):
    """Abstract base class for all SQL objects."""

    @abstractmethod
    def __str__(self) -> str:
        """Convert this SQL object into its string representation."""
        raise NotImplementedError
