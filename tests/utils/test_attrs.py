from typing import TypeVar

import pytest
from immutables import Map

from htap.utils.attrs import to_map, to_tuple

T = TypeVar("T")
U = TypeVar("U")


@pytest.mark.parametrize(
    "arg, expected",
    [
        pytest.param(1, 1, id="int"),
        pytest.param("ab", "ab", id="str"),
        pytest.param((1, 2), (1, 2), id="tuple"),
        pytest.param({"A", "B"}, {"A", "B"}, id="set"),
        pytest.param({1: "a", 2: "b"}, {1: "a", 2: "b"}, id="dict"),
        pytest.param({1: "a", 2: "b"}.keys(), {1: "a", 2: "b"}.keys(), id="dict_keys"),
    ],
)
def test_to_tuple_unconverted(arg: list[T] | U, expected: tuple[T] | U) -> None:
    assert to_tuple(arg) == expected


@pytest.mark.parametrize(
    "arg, expected",
    [
        pytest.param(["A", "B", "C"], ("A", "B", "C"), id="plain list"),
        pytest.param("a b c".split(), ("a", "b", "c"), id="function returning list"),
    ],
)
def test_to_tuple_converted(arg: list[T] | U, expected: tuple[T] | U) -> None:
    assert to_tuple(arg) == expected


K = TypeVar("K")
V = TypeVar("V")


@pytest.mark.parametrize(
    "arg, expected",
    [
        pytest.param(1, 1, id="int"),
        pytest.param("ab", "ab", id="str"),
        pytest.param((1, 2), (1, 2), id="tuple"),
        pytest.param(["A", "B"], ["A", "B"], id="list"),
        pytest.param({1, 2}, {1, 2}, id="set"),
        pytest.param(Map({1: "a", 2: "b"}), Map({1: "a", 2: "b"}), id="Map"),
    ],
)
def test_to_map_unconverted(arg: dict[K, V] | U, expected: Map[K, V] | U) -> None:
    assert to_map(arg) == expected


@pytest.mark.parametrize(
    "arg, expected",
    [
        pytest.param({1: "a", 2: "b"}, Map({1: "a", 2: "b"}), id="dict"),
    ],
)
def test_to_map_converted(arg: dict[K, V] | U, expected: Map[K, V] | U) -> None:
    assert to_map(arg) == expected
