"""
Workaround for sentinel types.

Adapted from https://stackoverflow.com/a/69243488/21451742
"""

from typing import Literal, final


class _SentinelMeta(type):
    def __repr__(cls) -> str:
        return f"<{cls.__name__}>"

    def __bool__(cls) -> Literal[False]:
        """Always treat as a Falsy value."""
        return False


class Sentinel(metaclass=_SentinelMeta):
    """Base class for sentinel types."""

    @final
    def __init__(self) -> None:
        raise RuntimeError("Sentinel classes should not be instantiated")
