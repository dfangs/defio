from __future__ import annotations

from abc import abstractmethod
from pathlib import Path
from typing import Generic, Protocol, TypeVar, cast, final

from attrs import define, field
from typing_extensions import override

from defio.infra.project.output import (
    HOST_KEY_SUFFIX,
    INITIAL_DBNAME_KEY_SUFFIX,
    PASSWORD_KEY_SUFFIX,
    PORT_KEY_SUFFIX,
    USERNAME_KEY_SUFFIX,
    PulumiStackOutputs,
)


class DbConfig(Protocol):
    """Represents the parameters of a DB connection."""

    host: str
    port: int
    username: str
    password: str
    dbname: str | None
    ssl_root_cert_path: Path | None

    @final
    def with_overrides(
        self,
        /,
        *,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        dbname: str | None = None,
        ssl_root_cert_path: Path | None = None,
    ) -> DbConfig:
        """
        Returns a new DB config with the given parameters overriden.
        """
        return OverridableDbConfig(
            base_config=self,
            host=host,
            port=port,
            username=username,
            password=password,
            dbname=dbname,
            ssl_root_cert_path=ssl_root_cert_path,
        )


@define(frozen=True)
class SimpleDbConfig(DbConfig):
    """Simple dataclass-based DB config."""

    host: str
    port: int
    username: str
    password: str
    dbname: str | None = None
    ssl_root_cert_path: Path | None = None


_T = TypeVar("_T", bound=DbConfig)


@define(frozen=True, eq=False, kw_only=True)
class OverridableDbConfig(DbConfig, Generic[_T]):
    """
    Generic overridable DB config.

    For each parameter, return the overridden value if specified,
    or otherwise use the value from the base config.
    """

    base_config: _T
    _host: str | None = field(default=None, alias="host")
    _port: int | None = field(default=None, alias="port")
    _username: str | None = field(default=None, alias="username")
    _password: str | None = field(default=None, alias="password")
    _dbname: str | None = field(default=None, alias="dbname")
    _ssl_root_cert_path: Path | None = field(default=None, alias="ssl_root_cert_path")

    @property
    @override
    def host(self) -> str:
        if self._host is not None:
            return self._host
        return self.base_config.host

    @property
    @override
    def port(self) -> int:
        if self._port is not None:
            return self._port
        return self.base_config.port

    @property
    @override
    def username(self) -> str:
        if self._username is not None:
            return self._username
        return self.base_config.username

    @property
    @override
    def password(self) -> str:
        if self._password is not None:
            return self._password
        return self.base_config.password

    @property
    @override
    def dbname(self) -> str | None:
        if self._dbname is not None:
            return self._dbname
        return self.base_config.dbname

    @property
    @override
    def ssl_root_cert_path(self) -> Path | None:
        if self._ssl_root_cert_path is not None:
            return self._ssl_root_cert_path
        return self.base_config.ssl_root_cert_path


@define
class PulumiDbConfig(DbConfig):
    """
    Abstract base class for DB configs derived from Pulumi stack outputs.
    """

    _stack_outputs: PulumiStackOutputs
    _key_prefix: str
    _db_identifier: str
    _db_name: str | None = None

    def __init__(
        self,
        stack_name: str,
        key_prefix: str,
        db_identifier: str,
        db_name: str | None = None,
    ) -> None:
        self._stack_outputs = PulumiStackOutputs(stack_name)
        self._key_prefix = key_prefix
        self._db_identifier = db_identifier
        self._db_name = db_name

    def _get_parameter(self, parameter: str) -> str:
        return self._stack_outputs.get(
            f"{self._key_prefix}:{self._db_identifier}:{parameter}"
        )

    @property
    @override
    def host(self) -> str:
        return self._get_parameter(HOST_KEY_SUFFIX)

    @property
    @override
    def port(self) -> int:
        return int(self._get_parameter(PORT_KEY_SUFFIX))

    @property
    @override
    def username(self) -> str:
        return self._get_parameter(USERNAME_KEY_SUFFIX)

    @property
    @override
    def password(self) -> str:
        return self._get_parameter(PASSWORD_KEY_SUFFIX)

    @property
    @override
    def dbname(self) -> str | None:
        if self._db_name is not None:
            return self._db_name

        # This may or may not be exported by Pulumi
        initial_dbname = cast(
            str | None,
            self._get_parameter(INITIAL_DBNAME_KEY_SUFFIX),
        )

        return initial_dbname

    @property
    @override
    @abstractmethod
    def ssl_root_cert_path(self) -> Path | None:
        raise NotImplementedError
