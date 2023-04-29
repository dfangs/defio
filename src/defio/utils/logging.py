from collections.abc import Callable, Iterator
from contextlib import contextmanager


@contextmanager
def log_around(
    verbose: bool,
    /,
    *,
    start: str | Callable[[], str],
    end: str | Callable[[], str],
) -> Iterator[None]:
    """
    Context manager that prints logging messages before and after
    the code block executes, if `enable` is set to `True`.
    """
    if verbose:
        print(start if isinstance(start, str) else start())

    yield

    if verbose:
        print(end if isinstance(end, str) else end())
