import time
from datetime import UTC, datetime, timedelta
from typing import Final
from unittest.mock import MagicMock, Mock, call
from zoneinfo import ZoneInfo

import pytest
from pytest_mock import MockerFixture

from defio.utils.time import (
    TimeMeasurement,
    is_datetime_offset_aware,
    log_time,
    measure_time,
)

SECONDS_TO_MICROSECONDS: Final = 1_000_000


@pytest.fixture(name="current_time")
def fixture_current_time() -> datetime:
    return datetime(year=2023, month=3, day=12)


@pytest.fixture(name="_mock_get_current_time")
def fixture_mock_get_current_time(
    mocker: MockerFixture, current_time: datetime
) -> MagicMock:
    return mocker.patch("defio.utils.time.get_current_time", return_value=current_time)


@pytest.fixture(name="mock_timer")
def fixture_mock_timer(mocker: MockerFixture) -> MagicMock:
    return mocker.spy(time, "perf_counter")


@pytest.fixture(name="mock_print")
def fixture_mock_print() -> Mock:
    return Mock(spec=print)


class TestTimeMeasurement:
    def test_start_time(
        self, _mock_get_current_time: MagicMock, current_time: datetime
    ) -> None:
        measurement = TimeMeasurement.start()
        assert measurement.start_time == current_time

    def test_end_time(
        self,
        _mock_get_current_time: MagicMock,
        current_time: datetime,
        mock_timer: MagicMock,
    ) -> None:
        measurement = TimeMeasurement.start(timer=mock_timer)
        start_time_benchmark = mock_timer.spy_return

        measurement.stop()
        end_time_benchmark = mock_timer.spy_return

        # End time should be measured using the given `timer()` function
        # up to microsecond resolution
        expected_end_time = current_time + timedelta(
            microseconds=int(
                (end_time_benchmark - start_time_benchmark) * SECONDS_TO_MICROSECONDS
            )
        )
        assert measurement.end_time == expected_end_time

    def test_elapsed_time(self) -> None:
        measurement = TimeMeasurement.start()
        measurement.stop()

        assert measurement.elapsed_time == measurement.end_time - measurement.start_time

    def test_total_seconds(self) -> None:
        measurement = TimeMeasurement.start()
        measurement.stop()

        assert measurement.total_seconds == measurement.elapsed_time.total_seconds()

    def test_unfinished(self) -> None:
        measurement = TimeMeasurement.start()

        with pytest.raises(ValueError):
            print(measurement.end_time)

        with pytest.raises(ValueError):
            print(measurement.elapsed_time)

        with pytest.raises(ValueError):
            print(measurement.total_seconds)


class TestMeasureTime:
    def test_ok(
        self,
        _mock_get_current_time: MagicMock,
        current_time: datetime,
        mock_timer: MagicMock,
    ) -> None:
        with measure_time(timer=mock_timer) as measurement:
            # `timer()` should be called once before entering the context...
            start_time_benchmark = mock_timer.spy_return

        # ... and once after exiting the context
        end_time_benchmark = mock_timer.spy_return

        assert measurement.start_time == current_time

        expected_end_time = current_time + timedelta(
            microseconds=int(
                (end_time_benchmark - start_time_benchmark) * SECONDS_TO_MICROSECONDS
            )
        )
        assert measurement.end_time == expected_end_time

    def test_bad(self) -> None:
        with pytest.raises(ValueError):
            with measure_time() as measurement:
                # Must not get the elapsed time within the block
                print(measurement.elapsed_time)


def test_log_time(mock_print: Mock, mock_timer: MagicMock) -> None:
    with log_time(
        True,
        start=(start := "start"),
        end=lambda m: str(m.total_seconds),
        logger=mock_print,
        timer=mock_timer,
    ):
        start_time_benchmark = mock_timer.spy_return
        mock_print.assert_called_once_with(start)

    end_time_benchmark = mock_timer.spy_return
    elapsed_time_seconds = (
        int((end_time_benchmark - start_time_benchmark) * SECONDS_TO_MICROSECONDS)
        / SECONDS_TO_MICROSECONDS
    )

    mock_print.assert_has_calls([call(start), call(str(elapsed_time_seconds))])


@pytest.mark.parametrize(
    "arg, expected",
    [
        (datetime(year=2023, month=3, day=12, tzinfo=ZoneInfo("Asia/Jakarta")), True),
        (datetime(year=2023, month=5, day=2, tzinfo=UTC), True),
        (datetime(year=2023, month=5, day=2), False),
    ],
)
def test_offset_aware(arg: datetime, expected: bool) -> None:
    assert is_datetime_offset_aware(arg) == expected
