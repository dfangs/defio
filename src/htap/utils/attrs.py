from typing import TypeVar

from immutables import Map

_T = TypeVar("_T")
_U = TypeVar("_U")


def to_tuple(arg: list[_T] | _U) -> tuple[_T] | _U:
    """
    Converts the argument into tuple if it is a list instance,
    or otherwise simply returns its unchanged.

    This function should only be used as a converter in `attrs.field`.

    NOTE: Only handle lists; do not handle all `Sequence` (e.g., `str`).
    """
    if isinstance(arg, list):
        return tuple(arg)
    return arg


_K = TypeVar("_K")
_V = TypeVar("_V")


# Same reasoning as above
def to_map(arg: dict[_K, _V] | _U) -> Map[_K, _V] | _U:
    """
    Converts the argument into `immutables.Map` if it is a dict instance,
    or otherwise simply returns its unchanged.

    This function should only be used as a converter in `attrs.field`.

    NOTE: Only handle dicts; do not handle all `Mapping`.
    """
    if isinstance(arg, dict):
        return Map(arg)
    return arg
