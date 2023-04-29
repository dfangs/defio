import time
from typing import Final
from unittest.mock import MagicMock

import pendulum
import pytest
from pytest_mock import MockerFixture

from htap.utils.time import TimeMeasurement, measure_time

CURRENT_TIME: Final = pendulum.datetime(year=2023, month=3, day=12)
SECONDS_TO_MICROSECONDS: Final = 1_000_000


@pytest.fixture(name="_mock_current_time")
def fixture_mock_current_time(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("htap.utils.time.get_current_time", return_value=CURRENT_TIME)


@pytest.fixture(name="mock_timer")
def fixture_mock_timer(mocker: MockerFixture) -> MagicMock:
    return mocker.spy(time, "perf_counter")


class TestTimeMeasurement:
    def test_start_time(self, _mock_current_time: MagicMock) -> None:
        measurement = TimeMeasurement.start()
        assert measurement.start_time == CURRENT_TIME

    def test_end_time(
        self, _mock_current_time: MagicMock, mock_timer: MagicMock
    ) -> None:
        measurement = TimeMeasurement.start(timer=mock_timer)
        start_time_benchmark = mock_timer.spy_return

        measurement.stop()
        end_time_benchmark = mock_timer.spy_return

        # End time should be measured using the given `timer()` function
        # up to microsecond resolution
        expected_end_time = CURRENT_TIME.add(
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
    def test_ok(self, _mock_current_time: MagicMock, mock_timer: MagicMock) -> None:
        with measure_time(timer=mock_timer) as measurement:
            # `timer()` should be called once before entering the context...
            start_time_benchmark = mock_timer.spy_return

        # ... and once after exiting the context
        end_time_benchmark = mock_timer.spy_return

        assert measurement.start_time == CURRENT_TIME

        expected_end_time = CURRENT_TIME.add(
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
