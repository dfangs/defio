from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock, Mock, call

import pytest
from pytest_mock import MockerFixture
from typing_extensions import override

from htap.client.config import PulumiDbConfig, SimpleDbConfig
from htap.infra.project.output import (
    HOST_KEY_SUFFIX,
    INITIAL_DBNAME_KEY_SUFFIX,
    PASSWORD_KEY_SUFFIX,
    PORT_KEY_SUFFIX,
    USERNAME_KEY_SUFFIX,
    PulumiStackOutputs,
)


@pytest.fixture(name="base_config")
def fixture_base_config() -> SimpleDbConfig:
    return SimpleDbConfig(
        host="localhost",
        port=5432,
        username="admin",
        password="112233",
        dbname="mit",
        ssl_root_cert_path=Path("some-path"),
    )


@pytest.mark.parametrize(
    "param_name, param_value",
    [
        pytest.param(None, None, id="no override"),
        pytest.param("host", "0.0.0.0", id="host"),
        pytest.param("port", 5439, id="port"),
        pytest.param("username", "tim", id="username"),
        pytest.param("password", "E%0@tz0W2S0R", id="password"),
        pytest.param("dbname", "library", id="dbname"),
        pytest.param(
            "ssl_root_cert_path", Path("actual-path"), id="ssl_root_cert_path"
        ),
    ],
)
def test_overridable_db_config(
    base_config: SimpleDbConfig,
    param_name: Literal[
        "host", "port", "username", "password", "dbname", "ssl_root_cert_path", None
    ],
    param_value: str | int | Path | None,
) -> None:
    if param_name is None:
        overriden_config = base_config.with_overrides()
    else:
        overriden_config = base_config.with_overrides(**{param_name: param_value})

    expected_host = param_value if param_name == "host" else base_config.host
    expected_port = param_value if param_name == "port" else base_config.port
    expected_username = (
        param_value if param_name == "username" else base_config.username
    )
    expected_password = (
        param_value if param_name == "password" else base_config.password
    )
    expected_dbname = param_value if param_name == "dbname" else base_config.dbname
    expected_ssl_root_cert_path = (
        param_value
        if param_name == "ssl_root_cert_path"
        else base_config.ssl_root_cert_path
    )

    assert overriden_config.host == expected_host
    assert overriden_config.port == expected_port
    assert overriden_config.username == expected_username
    assert overriden_config.password == expected_password
    assert overriden_config.dbname == expected_dbname
    assert overriden_config.ssl_root_cert_path == expected_ssl_root_cert_path


class SimplePulumiDbConfig(PulumiDbConfig):
    @property
    @override
    def ssl_root_cert_path(self) -> Path | None:
        return Path("good-path")


def test_pulumi_db_config(mocker: MockerFixture) -> None:
    mock_stack_outputs = Mock(
        spec=PulumiStackOutputs, get=Mock(return_value=MagicMock())
    )
    mock_stack_output_constructor = mocker.patch(
        "htap.client.config.PulumiStackOutputs", return_value=mock_stack_outputs
    )

    config = SimplePulumiDbConfig(
        stack_name=(stack_name := "main"),
        key_prefix=(key_prefix := "mit"),
        db_identifier=(db_identifier := "library"),
    )

    assert config.host is not None
    assert config.port is not None
    assert config.username is not None
    assert config.password is not None
    assert config.dbname is not None
    assert config.ssl_root_cert_path == Path("good-path")

    def key(name: str) -> str:
        return f"{key_prefix}:{db_identifier}:{name}"

    mock_stack_output_constructor.assert_called_once_with(stack_name)
    mock_stack_outputs.get.assert_has_calls(
        [
            call(key(HOST_KEY_SUFFIX)),
            call(key(PORT_KEY_SUFFIX)),
            call(key(USERNAME_KEY_SUFFIX)),
            call(key(PASSWORD_KEY_SUFFIX)),
            call(key(INITIAL_DBNAME_KEY_SUFFIX)),
        ]
    )
