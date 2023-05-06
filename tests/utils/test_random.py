import math
from collections.abc import Sequence
from typing import Any

import pytest

from defio.utils.random import Randomizer


@pytest.mark.parametrize("p", [0, 0.5, 1])
def test_flip_ok(p: float) -> None:
    flip = Randomizer().flip(p)
    assert flip or not flip


@pytest.mark.parametrize("p", [-0.5, 1.5, math.nan])
def test_flip_bad(p: float) -> None:
    with pytest.raises(ValueError):
        Randomizer().flip(p)


@pytest.mark.parametrize(
    "array, weights",
    [
        (["a"], None),
        (["a", "b"], [0.2, 0.8]),
    ],
)
def test_choose_one_ok(array: Sequence[Any], weights: Sequence[float] | None) -> None:
    chosen_one = Randomizer().choose_one(array, weights=weights)
    assert chosen_one in array


@pytest.mark.parametrize(
    "array, weights",
    [
        pytest.param([], None, id="empty array"),
        pytest.param(["a", "b"], [0.7, 0.8], id="total weights more than one"),
        pytest.param(["a"], [0.3, 0.7], id="too many weights"),
    ],
)
def test_choose_one_bad(array: Sequence[Any], weights: Sequence[float] | None) -> None:
    with pytest.raises(ValueError):
        Randomizer().choose_one(array, weights=weights)


@pytest.mark.parametrize(
    "array, size, replace, weights",
    [
        (["a"], 1, False, None),
        (["a"], 5, True, None),
        (["a", "b"], 1, False, [0.3, 0.7]),
        (["a", "b"], 2, True, [0.3, 0.7]),
    ],
)
def test_choose_ok(
    array: Sequence[Any], size: int, replace: bool, weights: Sequence[float] | None
) -> None:
    chosen = Randomizer().choose(array, size=size, replace=replace, weights=weights)
    assert len(chosen) == size

    seen = set()
    for chosen_one in chosen:
        assert chosen_one in array
        if not replace:
            assert chosen_one not in seen
        seen.add(chosen_one)


@pytest.mark.parametrize(
    "array, size, replace, weights",
    [
        pytest.param([], 1, True, None, id="empty array"),
        pytest.param([], 0, True, None, id="size less than one"),
        pytest.param(["a"], 2, False, None, id="size too large without replacement"),
        pytest.param(["a", "b"], 2, True, [0.7, 0.8], id="total weights more than one"),
        pytest.param(["a"], 2, True, [0.3, 0.7], id="too many weights"),
    ],
)
def test_choose_bad(
    array: Sequence[Any], size: int, replace: bool, weights: Sequence[float] | None
) -> None:
    with pytest.raises(ValueError):
        Randomizer().choose(array, size=size, replace=replace, weights=weights)


@pytest.mark.parametrize(
    "low, high, inclusive",
    [
        (1, None, False),
        (0, None, True),
        (1, 2, False),
        (3, 3, True),
        (-1, 1, False),
    ],
)
def test_randint_ok(low: int, high: int | None, inclusive: bool) -> None:
    if high is not None:
        chosen = Randomizer().randint(low, high, inclusive=inclusive)
    else:
        chosen = Randomizer().randint(low, inclusive=inclusive)

    if inclusive:
        if high is not None:
            assert low <= chosen <= high
        else:
            assert 0 <= chosen <= low
    else:
        if high is not None:
            assert low <= chosen < high
        else:
            assert 0 <= chosen < low


@pytest.mark.parametrize(
    "low, high, inclusive",
    [
        (0, None, False),
        (-1, None, True),
        (1, 1, False),
        (1, 0, True),
    ],
)
def test_randint_bad(low: int, high: int | None, inclusive: bool) -> None:
    with pytest.raises(ValueError):
        if high is not None:
            Randomizer().randint(low, high, inclusive=inclusive)
        else:
            Randomizer().randint(low, inclusive=inclusive)
