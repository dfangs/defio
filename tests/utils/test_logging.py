from unittest.mock import Mock, call

import pytest

from defio.utils.logging import log_around


@pytest.fixture(name="mock_print")
def fixture_mock_print() -> Mock:
    return Mock(spec=print)


def test_log_around_true(mock_print: Mock) -> None:
    with log_around(True, start=(start := "start"), end=lambda: end, logger=mock_print):
        mock_print.assert_called_once_with(start)
        end = "end"  # Late binding

    mock_print.assert_has_calls([call(start), call(end)])


def test_log_around_false(mock_print: Mock) -> None:
    with log_around(False, start="start", end="end", logger=mock_print):
        ...

    mock_print.assert_not_called()
