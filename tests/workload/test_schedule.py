from datetime import datetime, timedelta
from itertools import count
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from defio.workload.schedule import Once, Repeat


@pytest.fixture(name="initial_time")
def fixture_initial_time() -> datetime:
    return datetime(year=2023, month=3, day=12)


@pytest.fixture(name="current_time_diff")
def fixture_current_time_diff() -> timedelta:
    return timedelta(seconds=1)


@pytest.fixture(name="_mock_get_current_time")
def fixture_mock_get_current_time(
    mocker: MockerFixture, initial_time: datetime, current_time_diff: timedelta
) -> MagicMock:
    return mocker.patch(
        "defio.workload.schedule.get_current_time",
        side_effect=(initial_time + i * current_time_diff for i in count()),
    )


class TestOnce:
    def test_time_until_next(
        self,
        _mock_get_current_time: MagicMock,
        initial_time: datetime,
        current_time_diff: timedelta,
    ) -> None:
        scheduled_time = datetime(year=2023, month=5, day=2)

        TestOnce._assert_once(
            once=Once(at=scheduled_time),
            scheduled_time=scheduled_time,
            initial_time=initial_time,
            current_time_diff=current_time_diff,
        )

    def test_now(
        self,
        _mock_get_current_time: MagicMock,
        initial_time: datetime,
        current_time_diff: timedelta,
    ) -> None:
        TestOnce._assert_once(
            once=Once.now(),
            scheduled_time=initial_time,  # This already calls `get_current_time()` once
            initial_time=initial_time + current_time_diff,
            current_time_diff=current_time_diff,
        )

    @staticmethod
    def _assert_once(
        once: Once,
        scheduled_time: datetime,
        initial_time: datetime,
        current_time_diff: timedelta,
        num_iters: int = 3,
    ) -> None:
        assert once.at == scheduled_time

        for i in range(num_iters):
            assert once.time_until_next() == scheduled_time - (
                initial_time + i * current_time_diff
            )


class TestRepeat:
    def test_time_until_next(
        self,
        _mock_get_current_time: MagicMock,
        initial_time: datetime,
        current_time_diff: timedelta,
    ) -> None:
        repeat = Repeat(
            interval=1.5 * current_time_diff,
            start_time=initial_time + 1.5 * current_time_diff,
            end_time=initial_time + 4.5 * current_time_diff,
        )

        # Before `start_time`
        assert repeat.time_until_next() == 1.5 * current_time_diff
        assert repeat.time_until_next() == 0.5 * current_time_diff

        # Between `start_time` and `end_time`
        assert repeat.time_until_next() == 1 * current_time_diff
        assert repeat.time_until_next() == timedelta(0)
        assert repeat.time_until_next() == 0.5 * current_time_diff

        # After `end_time`
        assert repeat.time_until_next() == -0.5 * current_time_diff
        assert repeat.time_until_next() == -1.5 * current_time_diff

    def test_starting_now_with_num_repeat(
        self, _mock_get_current_time: MagicMock, current_time_diff: timedelta
    ) -> None:
        repeat = Repeat.starting_now(
            interval=1.5 * current_time_diff,
            num_repeat=2,
        )

        # Before first event
        assert repeat.time_until_next() == 0.5 * current_time_diff

        # After first event, before second event
        assert repeat.time_until_next() == 1 * current_time_diff

        # At second event
        assert repeat.time_until_next() == timedelta(0)

        # After second event
        assert repeat.time_until_next() == -1 * current_time_diff

    def test_starting_now_with_end_time(
        self, _mock_get_current_time: MagicMock, current_time_diff: timedelta
    ) -> None:
        # Default `end_time` = basically forever
        repeat = Repeat.starting_now(interval=1.5 * current_time_diff)

        num_iters = 3
        for _ in range(num_iters):
            assert repeat.time_until_next() == 0.5 * current_time_diff
            assert repeat.time_until_next() == 1 * current_time_diff
            assert repeat.time_until_next() == timedelta(0)
