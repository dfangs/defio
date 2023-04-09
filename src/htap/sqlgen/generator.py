from abc import abstractmethod
from collections.abc import Iterator

from typing_extensions import override

from htap.utils.generator import ImmutableGenerator


class SqlGenerator(ImmutableGenerator[str]):
    """
    Abstract base class for an immutable "generator" that yields a
    (potentially unbounded) sequence of SQL strings.
    """

    @override
    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        raise NotImplementedError
