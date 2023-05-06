from __future__ import annotations

from collections.abc import Sequence
from math import isclose
from typing import TypeVar, final

import numpy as np
from attrs import define

_T = TypeVar("_T")


@final
@define
class Randomizer:
    """Helper class to perform RNG-related tasks."""

    _rng: np.random.Generator

    def __init__(self, seed: int | None = None) -> None:
        self._rng = np.random.default_rng(seed)

    def flip(self, p: float) -> bool:
        """
        Returns `True` with probability `p`, or `False` otherwise.

        Raises a `ValueError` if `p` is not in [0, 1].
        """
        if not 0 <= p <= 1:
            raise ValueError("`p` must be a valid probability")

        return self._rng.random() < p

    def choose_one(
        self, array: Sequence[_T], /, *, weights: Sequence[float] | None = None
    ) -> _T:
        """
        Chooses a single element from the given array with discrete probability
        distribution `weights`, or uniformly at random if `weights` is `None`.

        Raises a `ValueError` if the given array is empty, if `weights` does not
        sum up to one, or if `weights` and `array` have mismatched lengths.
        """
        if len(array) == 0:
            raise ValueError("`array` must not be empty")

        return self.choose(array, weights=weights)[0]

    def choose(
        self,
        array: Sequence[_T],
        *,
        size: int = 1,
        replace: bool = False,
        weights: Sequence[float] | None = None,
    ) -> Sequence[_T]:
        """
        Randomly chooses `size` elements with or without replacement (depending on
        `replace`) from the given array with a discrete probability distribution
        `weights`, or uniformly at random if `weights` is `None`.

        Raises a `ValueError` if `size` is less than one, or if there are not
        enough elements to choose from `array`.
        """
        if size < 1:
            raise ValueError("`size` must be at least one")
        if replace and len(array) == 0:
            raise ValueError("`array` must not be empty")
        if not replace and size > len(array):
            raise ValueError("`array` must have enough elements")
        if weights is not None:
            if not isclose(sum(weights), 1):
                raise ValueError("`weights` must sum up to one")
            if len(weights) != len(array):
                raise ValueError("Length of `weights` must match the given array")

        indexes = self._rng.choice(
            np.arange(len(array)), size=size, replace=replace, p=weights
        )

        return [array[i] for i in indexes]

    def randint(
        self, low: int, high: int | None = None, /, *, inclusive: bool = False
    ) -> int:
        """
        Returns an integer chosen uniformly at random from the half-open
        interval [`low`, `high`).

        If `high` is `None`, the interval is [`0`, `low`) instead.

        If `inclusive` is `True`, the interval is closed instead.

        Raises a `ValueError` if the interval is invalid.
        """
        try:
            return int(self._rng.integers(low, high, endpoint=inclusive))
        except ValueError as exc:
            if high is None:
                raise ValueError(f"Invalid interval: [0, {low})") from exc
            raise ValueError(f"Invalid interval: [{low}, {high})") from exc
