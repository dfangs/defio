from collections.abc import Callable, Iterator
from contextlib import contextmanager


@contextmanager
def log_around(
    verbose: bool,
    /,
    *,
    start: str | Callable[[], str],
    end: str | Callable[[], str],
    logger: Callable[[str], None] = print,
) -> Iterator[None]:
    """
    Context manager that logs the given messages before and after
    the code block executes, if `verbose` is set to `True`.
    """
    if verbose:
        logger(start if isinstance(start, str) else start())

    yield

    if verbose:
        logger(end if isinstance(end, str) else end())
