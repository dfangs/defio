import pytest

from defio.utils.sentinel import Sentinel


class DummySentinel(Sentinel):
    ...


def test_sentinel_constructor() -> None:
    with pytest.raises(RuntimeError):
        DummySentinel()


def test_sentinel_equality() -> None:
    assert DummySentinel is DummySentinel
