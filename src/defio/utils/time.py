from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import final

import pendulum
from attrs import define
from pendulum import UTC, DateTime, Period

from defio.utils.logging import log_around  # type: ignore

_SECONDS_TO_MICROSECONDS = 1_000_000


@final
@define
class TimeMeasurement:
    """
    Represents a measurement of some time interval up to microsecond resolution.
    """

    start_time: DateTime
    _start_time_benchmark: float
    _end_time_benchmark: float | None
    _timer: Callable[[], float]

    def __init__(
        self, /, *, start_time: DateTime, timer: Callable[[], float] = time.perf_counter
    ) -> None:
        # Get actual time from the input
        self.start_time = start_time

        # Use timer function (e.g., `perf_counter()`) for more accurate duration
        self._start_time_benchmark = timer()
        self._end_time_benchmark = None

        # Make this customizable, in case a different level of precision is desired
        self._timer = timer

    @staticmethod
    def start(timer: Callable[[], float] = time.perf_counter) -> TimeMeasurement:
        """Starts a new time measurement now."""
        return TimeMeasurement(start_time=get_current_time(), timer=timer)

    def stop(self) -> None:
        """Stops this time measurement and records the end time."""
        self._end_time_benchmark = self._timer()

    @property
    def end_time(self) -> DateTime:
        """
        Returns the end time of this measurement.

        Raises a `ValueError` if the measurement has not finished.
        """
        if self._end_time_benchmark is None:
            raise ValueError("Measurement has not finished yet")

        return self.start_time.add(
            microseconds=int(
                (self._end_time_benchmark - self._start_time_benchmark)
                * _SECONDS_TO_MICROSECONDS
            )
        )

    @property
    def elapsed_time(self) -> Period:
        """
        Returns the elapsed time between the start time and end time
        of this time measurement.

        Raises a `ValueError` if the measurement has not finished.
        """
        return self.end_time - self.start_time

    @property
    def total_seconds(self) -> float:
        """
        Returns the total seconds of the elapsed time of this measurement.

        Raises a `ValueError` if the measurement has not finished.
        """
        return self.elapsed_time.total_seconds()


@contextmanager
def measure_time(
    *, timer: Callable[[], float] = time.perf_counter
) -> Iterator[TimeMeasurement]:
    """
    Context manager that measures the elapsed time over the code block.

    Usage:
    ```
    with measure_time() as measurement:
        ...  # Do some work here

    # Get measurement
    print(measurement.elapsed_time)
    print(f"{measurement.total_seconds:.2f} seconds")
    ```
    """
    measurement = TimeMeasurement.start(timer)
    try:
        yield measurement
    finally:
        measurement.stop()


@contextmanager
def log_time(
    verbose: bool,
    /,
    *,
    start: str | Callable[[TimeMeasurement], str],
    end: str | Callable[[TimeMeasurement], str],
    logger: Callable[[str], None] = print,
    timer: Callable[[], float] = time.perf_counter,
) -> Iterator[None]:
    """
    Wrapper context manager for `log_around` and `measure_time`.

    This context manager measures the elapsed time over the code block.
    If `verbose is set to `True`, it also logs the given start and end
    messages before and after the code block executes, respectively.
    """
    with log_around(
        verbose,
        start=start if isinstance(start, str) else lambda: start(measurement),
        end=end if isinstance(end, str) else lambda: end(measurement),
        logger=logger,
    ):
        with measure_time(timer=timer) as measurement:
            yield


def get_current_time() -> DateTime:
    """Returns the current time in UTC."""
    return pendulum.now(tz=UTC)


def get_event_loop_time() -> float:
    """
    Returns the current internal time of the running event loop.

    In general, the returned value should be monotonic, and thus
    more appropriate for computing time intervals in the context
    of asyncio.
    """
    return asyncio.get_running_loop().time()
