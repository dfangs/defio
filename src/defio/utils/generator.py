from abc import ABC, abstractmethod
from collections.abc import Iterator, Sequence
from typing import Generic, TypeVar, final

from attrs import define, field
from typing_extensions import override

from defio.utils.attrs import to_tuple

_T = TypeVar("_T")


class ImmutableGenerator(ABC, Generic[_T]):
    """
    Abstract base class for an immutable "generator" (or rather, iterator)
    that yields a (potentially unbounded) sequence of values.

    It implements the `Iterable` protocol, but unlike other iterables
    in general, it can be safely iterated multiple times due to its
    immutability. This helps avoid having to deal with copying
    (unbounded) iterators or generators.

    Note that this abstract base class offers no mechanism to enforce
    this property, and thus the implementing classes are responsible
    for satisfying the contract.
    """

    @abstractmethod
    def __iter__(self) -> Iterator[_T]:
        raise NotImplementedError


@final
@define(frozen=True)
class _ChainedImmutableGenerator(ImmutableGenerator[_T]):
    """
    Immutable generator that consists of multiple subgenerators.

    This class is intended to mimic the behavior of `itertools.chain()`,
    except that we need to implement our own chaining mechanism due to
    using `ImmutableGenerator` instead of normal iterables.
    """

    _subgenerators: Sequence[Sequence[_T] | ImmutableGenerator[_T]]

    def __init__(
        self, subgenerators: Sequence[Sequence[_T] | ImmutableGenerator[_T]]
    ) -> None:
        # Can't use direct assignment due to frozen dataclass
        object.__setattr__(
            self,
            "_subgenerators",
            tuple(to_tuple(subgenerator) for subgenerator in subgenerators),
        )

    @override
    def __iter__(self) -> Iterator[_T]:
        for subgenerator in self._subgenerators:
            yield from subgenerator


@final
@define(frozen=True)
class _SlicedImmutableGenerator(ImmutableGenerator[_T]):
    """
    A finite slice of another immutable generator.

    TODO: Generalize (e.g., optional `stop`).
    """

    _subgenerator: Sequence[_T] | ImmutableGenerator[_T] = field(
        converter=to_tuple, alias="subgenerator"  # type: ignore (TODO)
    )
    start: int
    stop: int

    @override
    def __iter__(self) -> Iterator[_T]:
        for i, value in enumerate(self._subgenerator):
            if i < self.start:
                continue
            if i >= self.stop:
                return

            yield value


def chain(
    *subgenerators: Sequence[_T] | ImmutableGenerator[_T],
) -> ImmutableGenerator[_T]:
    """
    Chains the given subgenerators (or finite sequences, for convenience)
    in the specified order into a single immutable generator.
    """
    return _ChainedImmutableGenerator(subgenerators)


def chunk(
    generator: Sequence[_T] | ImmutableGenerator[_T], num_chunks: int, chunk_size: int
) -> Sequence[ImmutableGenerator[_T]]:
    """
    Splits the given generator into `num_chunks` "chunks" of size `chunk_size`.
    """
    return [
        _SlicedImmutableGenerator(generator, i * chunk_size, (i + 1) * chunk_size)
        for i in range(num_chunks)
    ]
