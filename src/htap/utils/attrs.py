from typing import TypeVar

from immutables import Map

_T = TypeVar("_T")
_U = TypeVar("_U")


# NOTE: Only handle lists; don't use sequence (e.g., str is also a sequence)
def to_tuple(arg: list[_T] | _U) -> tuple[_T] | _U:
    if isinstance(arg, list):
        return tuple(arg)
    return arg


_K = TypeVar("_K")
_V = TypeVar("_V")


# Same reasoning as above
def to_map(arg: dict[_K, _V] | _U) -> Map[_K, _V] | _U:
    if isinstance(arg, dict):
        return Map(arg)
    return arg
