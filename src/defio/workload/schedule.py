from __future__ import annotations

from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Protocol, final, overload

from attrs import define
from typing_extensions import override

from defio.utils.time import get_current_time


class Schedule(Protocol):
    """
    Represents a schedule for executing some event.

    Conceptually similar to a cron expression, but designed
    to be simpler for typical database workloads.
    """

    @abstractmethod
    def time_until_next(self) -> timedelta:
        """
        Returns the remaining time until the next event in which
        the schedule is fired.

        If the last event of the schedule is already in the past,
        the return value will be negative.
        """
        raise NotImplementedError


@final
@define(frozen=True)
class Once(Schedule):
    """
    Schedules an event at one particular time.
    """

    at: datetime

    @override
    def time_until_next(self) -> timedelta:
        return self.at - get_current_time()

    @staticmethod
    def now() -> Once:
        """
        Returns a `Once` schedule at the current time.

        Note that this means future calls to `time_until_next()`
        will definitely return zero (assuming monotonic clock).
        """
        return Once(at=get_current_time())


@final
@define(frozen=True, kw_only=True)
class Repeat(Schedule):
    """
    Schedules an event to repeat every `interval` period of time,
    starting from `start_time` and ending at `end_time`.
    """

    interval: timedelta
    start_time: datetime
    end_time: datetime = datetime.max

    def __attrs_post_init__(self) -> None:
        assert self.start_time <= self.end_time

    @override
    def time_until_next(self) -> timedelta:
        current_time = get_current_time()

        # Case 1: Schedule has not started
        if current_time <= self.start_time:
            return self.start_time - current_time

        # Case 2: Schedule has ended
        if current_time > self.end_time:
            return self.end_time - current_time

        # Case 3: Schedule is in progress
        elapsed_time = current_time - self.start_time
        time_since_last_event = elapsed_time % self.interval
        time_until_next_event = (self.interval - time_since_last_event) % self.interval

        return min(time_until_next_event, self.end_time - current_time)

    @overload
    @staticmethod
    def starting_now(*, interval: timedelta, num_repeat: int) -> Repeat:
        """
        Returns a `Repeat` schedule that starts at the current time
        and ends after `num_repeat` repeats.
        """

    @overload
    @staticmethod
    def starting_now(
        *, interval: timedelta, end_time: datetime = datetime.max
    ) -> Repeat:
        """
        Returns a `Repeat` schedule that starts at the current time
        and ends at `end_time`.
        """

    @staticmethod
    def starting_now(
        *,
        interval: timedelta,
        end_time: datetime = datetime.max,
        num_repeat: int | None = None,
    ) -> Repeat:
        start_time = get_current_time()

        # First overload: `num_repeat`
        if num_repeat is not None:
            if num_repeat < 1:
                raise ValueError("`num_repeat` must be at least 1")

            return Repeat(
                interval=interval,
                start_time=start_time,
                end_time=start_time + num_repeat * interval,
            )

        # Second overload: `end_time`
        assert end_time is not None
        return Repeat(
            interval=interval,
            start_time=start_time,
            end_time=end_time,
        )
