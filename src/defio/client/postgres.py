from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Self

import psycopg
from attrs import define, field
from typing_extensions import override

from defio.client import AsyncClient, AsyncConnection
from defio.client.config import DbConfig


@define
class PostgresConnection(AsyncConnection[tuple[Any, ...]]):
    """
    Asynchronous connection to Postgres-compatible databases.
    """

    _aconn: psycopg.AsyncConnection[tuple[Any, ...]] = field(alias="aconn")
    _closed: bool = field(default=False, init=False)

    @staticmethod
    async def create(
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        dbname: str | None = None,
        ssl_root_cert_path: Path | None = None,
    ) -> PostgresConnection:
        """
        Creates a Postgres async connection with the given connection parameters.

        If `ssl_root_cert_path` is provided, use SSL when connecting to the database.
        This will fail if the database doesn't have the right server certificate
        (i.e. it must be verifiable using the given root CA).
        """
        if ssl_root_cert_path is not None and not ssl_root_cert_path.exists():
            raise ValueError("Path to the CA certificate does not exist")

        optional_args = dict(
            dbname=dbname,
            # `verify-ca` is stronger than `require`
            sslmode=("verify-ca" if ssl_root_cert_path is not None else None),
            sslrootcert=(
                str(ssl_root_cert_path.resolve())
                if ssl_root_cert_path is not None
                else None
            ),
        )

        # No need to enter the psycopg connection since
        # this class' own context manager will take care of it
        aconn = await psycopg.AsyncConnection.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            **{k: v for k, v in optional_args.items() if v is not None},
            # Simply `execute()` statements one-by-one without starting any transactions
            # See https://www.psycopg.org/psycopg3/docs/basic/transactions.html#autocommit-transactions
            autocommit=True,
        )

        return PostgresConnection(aconn=aconn)

    @override
    async def close(self) -> None:
        if self._closed:
            raise ValueError("Connection is already closed")

        await self._aconn.close()
        self._closed = True

    @override
    async def execute(self, query: str) -> AsyncIterator[tuple[Any, ...]]:
        # Psycopg essentially only accepts `LiteralString`, so disable type check here
        cursor = await self._aconn.execute(query)  # type: ignore

        # If the last operation did not produce any results,
        # iterating over the cursor will raise an exception
        if cursor.rownumber is None:
            return

        async for row in cursor:
            yield row

    @override
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        async with self._aconn.transaction():
            yield


@define(frozen=True, kw_only=True)
class PostgresClient(AsyncClient[tuple[Any, ...]]):
    """
    Asynchronous client for Postgres-compatible databases.
    """

    host: str
    port: int
    username: str
    password: str
    dbname: str | None = None
    ssl_root_cert_path: Path | None = None

    @classmethod
    def from_config(cls, config: DbConfig) -> Self:
        return cls(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            dbname=config.dbname,
            ssl_root_cert_path=config.ssl_root_cert_path,
        )

    @override
    async def connect(self) -> PostgresConnection:
        return await PostgresConnection.create(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            dbname=self.dbname,
            ssl_root_cert_path=self.ssl_root_cert_path,
        )
