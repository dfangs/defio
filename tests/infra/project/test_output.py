from typing import Final
from unittest.mock import MagicMock, Mock

import pytest
from pulumi import automation as auto
from pulumi.automation import OutputValue, Stack
from pytest_mock import MockerFixture

from htap.infra.project.output import PULUMI_PROJECT_PATH, PulumiStackOutputs

OUTPUT_MAP: Final = {
    "host": "localhost",
    "port": "5432",
}


class TestPulumiStackOutputs:
    @pytest.fixture(name="_mock_stack")
    def fixture_pulumi_stack(self) -> Mock:
        return Mock(
            spec=Stack,
            outputs=Mock(
                return_value={
                    k: Mock(spec=OutputValue, value=v) for k, v in OUTPUT_MAP.items()
                }
            ),
        )

    @pytest.fixture(name="_mock_select_stack")
    def fixture_pulumi_auto_select_stack(
        self, mocker: MockerFixture, _mock_stack: Mock
    ) -> MagicMock:
        return mocker.patch(
            "htap.infra.project.output.auto.select_stack",
            return_value=_mock_stack,
        )

    def test_ok(self, _mock_select_stack: MagicMock, _mock_stack: MagicMock) -> None:
        stack_outputs = PulumiStackOutputs("main")
        for k, v in OUTPUT_MAP.items():
            # Get each key twice
            assert stack_outputs.get(k) == v
            assert stack_outputs.get(k) == v

        # These methods should only be called once (i.e. not at each call to `get()`)
        _mock_select_stack.assert_called_once_with(
            stack_name="main", work_dir=str(PULUMI_PROJECT_PATH.resolve())
        )
        _mock_stack.outputs.assert_called_once()

    def test_get_key_not_exists(
        self, _mock_select_stack: MagicMock, _mock_stack: MagicMock
    ) -> None:
        stack_outputs = PulumiStackOutputs("main")
        with pytest.raises(KeyError):
            stack_outputs.get("password")

    def test_stack_not_exists(self, mocker: MockerFixture) -> None:
        # Only patch `select_stack()` and not the whole `auto` module
        # Otherwise, it will inadvertently patch the exception class as well
        mock_select_stack = mocker.patch(
            "htap.infra.project.output.auto.select_stack",
            side_effect=auto.errors.StackNotFoundError(MagicMock()),
        )

        with pytest.raises(ValueError):
            PulumiStackOutputs("dev")

        mock_select_stack.assert_called_once_with(
            stack_name="dev", work_dir=str(PULUMI_PROJECT_PATH.resolve())
        )
