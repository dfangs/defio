from unittest.mock import MagicMock, call

import pytest
from pytest_mock import MockerFixture

from htap.utils.logging import log_around


@pytest.fixture(name="mock_print")
def fixture_mock_print(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("htap.utils.logging.print")


def test_log_around_true(mock_print: MagicMock) -> None:
    with log_around(True, start=(start := "start"), end=lambda: end):
        end = "end"  # Late binding

    mock_print.assert_has_calls([call(start), call(end)])


def test_log_around_false(mock_print: MagicMock) -> None:
    with log_around(False, start="start", end="end"):
        ...

    mock_print.assert_not_called()
