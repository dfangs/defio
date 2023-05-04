from collections.abc import Iterator
from typing import Final, final

from attr import define
from typing_extensions import override

from defio.utils.generator import ImmutableGenerator, chain

NUM_ITERS: Final = 3


@final
@define(frozen=True)
class IntRangeGenerator(ImmutableGenerator[int]):
    low: int
    high: int

    @override
    def __iter__(self) -> Iterator[int]:
        yield from range(self.low, self.high)


def test_generator() -> None:
    range_max = 5
    generator = IntRangeGenerator(0, range_max)

    # Immutable generators can be iterated multiple times
    for _ in range(NUM_ITERS):
        assert list(generator) == list(range(range_max))


def test_chain_self() -> None:
    range_max = 5
    num_subgenerators = 4

    subgenerator = IntRangeGenerator(0, range_max)
    chained = chain(*[subgenerator for _ in range(num_subgenerators)])

    # Chained generators can also be iterated multiple times
    for _ in range(NUM_ITERS):
        assert list(chained) == 4 * list(range(range_max))


def test_chain_mixed() -> None:
    range_max = 5
    num_subgenerators = 4

    subgenerators = [
        (
            IntRangeGenerator(i * range_max, (i + 1) * range_max)
            if i % 2 == 0
            else list(range(i * range_max, (i + 1) * range_max))
        )
        for i in range(num_subgenerators)
    ]

    chained = chain(*subgenerators)

    # Chained generators can also be iterated multiple times
    for _ in range(NUM_ITERS):
        assert list(chained) == list(range(num_subgenerators * range_max))
