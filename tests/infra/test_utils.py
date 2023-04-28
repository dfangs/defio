from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from htap.constants import PACKAGE_NAME
from htap.infra.utils import ComponentMixin, get_az


def test_component_mixin() -> None:
    class SomeResource(ComponentMixin):
        ...

    assert (
        SomeResource().get_type_name()
        == f"{PACKAGE_NAME}:{Path(__file__).stem}:{SomeResource.__name__}"
    )


@pytest.fixture(name="_mock_azs")
def fixture_mock_get_available_azs(mocker: MockerFixture) -> None:
    # Note: For simplicity, only mock the `get_available_azs()` function
    # Mocking the whole `pulumi_aws` would be slower and rather unnecessary
    # (but as a consequence, we don't test the other helper functions)
    mocker.patch(
        "htap.infra.utils.get_available_azs",
        return_value=["us-east-1a", "us-east-1b"],
    )


@pytest.mark.parametrize(
    "index, wrap_around, expected",
    [
        (0, False, "us-east-1a"),
        (1, False, "us-east-1b"),
        (-1, True, "us-east-1b"),
        (0, True, "us-east-1a"),
        (1, True, "us-east-1b"),
        (2, True, "us-east-1a"),
    ],
)
def test_get_az_ok(_mock_azs, index: int, wrap_around: bool, expected: str) -> None:
    assert get_az(index, wrap_around) == expected


@pytest.mark.parametrize(
    "index, wrap_around",
    [
        (-1, False),
        (2, False),
    ],
)
def test_get_az_invalid_index(_mock_azs, index: int, wrap_around: bool) -> None:
    with pytest.raises(IndexError):
        get_az(index, wrap_around)
