from abc import abstractmethod
from collections.abc import Mapping
from functools import cached_property
from pathlib import Path
from typing import Any, Protocol

from attr import define
from pulumi import automation as auto
from typing_extensions import override

from htap.constants import (
    HOST_KEY_SUFFIX,
    INITIAL_DBNAME_KEY_SUFFIX,
    PASSWORD_KEY_SUFFIX,
    PORT_KEY_SUFFIX,
    USERNAME_KEY_SUFFIX,
)


class DbConfig(Protocol):
    """
    Represents the parameters for a DB connection.
    """

    host: str
    port: int
    username: str
    password: str
    dbname: str
    ssl_root_cert_path: Path


@define(frozen=True)
class PulumiDbConfig(DbConfig):
    """
    Abstract base class for a DB config derived from Pulumi stack outputs.
    """

    project_path: Path
    stack_name: str
    db_identifier: str
    db_name: str | None = None

    def __attrs_post_init__(self) -> None:
        try:
            # Load stack eagerly
            self._stack_outputs
        except Exception as exc:
            raise ValueError("Could not load Pulumi Stack") from exc

    @cached_property
    def _stack_outputs(self) -> Mapping[str, Any]:
        stack = auto.select_stack(
            stack_name=self.stack_name, work_dir=str(self.project_path.resolve())
        )
        return {key: output.value for key, output in stack.outputs().items()}

    @property
    @override
    def host(self) -> str:
        return self._stack_outputs[
            f"{self.key_prefix}:{self.db_identifier}:{HOST_KEY_SUFFIX}"
        ]

    @property
    @override
    def port(self) -> int:
        return int(
            self._stack_outputs[
                f"{self.key_prefix}:{self.db_identifier}:{PORT_KEY_SUFFIX}"
            ]
        )

    @property
    @override
    def username(self) -> str:
        return self._stack_outputs[
            f"{self.key_prefix}:{self.db_identifier}:{USERNAME_KEY_SUFFIX}"
        ]

    @property
    @override
    def password(self) -> str:
        return self._stack_outputs[
            f"{self.key_prefix}:{self.db_identifier}:{PASSWORD_KEY_SUFFIX}"
        ]

    @property
    @override
    def dbname(self) -> str:
        if self.db_name is not None:
            return self.db_name

        initial_dbname = self._stack_outputs[
            f"{self.key_prefix}:{self.db_identifier}:{INITIAL_DBNAME_KEY_SUFFIX}"
        ]

        # This may not be available in Pulumi
        if initial_dbname is not None:
            return initial_dbname

        raise ValueError("dbname is not provided by either user or Pulumi")

    @property
    @abstractmethod
    @override
    def ssl_root_cert_path(self) -> Path:
        raise NotImplementedError

    @property
    @abstractmethod
    def key_prefix(self) -> str:
        raise NotImplementedError
