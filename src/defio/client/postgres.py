from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Final, Self

import psycopg
from attrs import define, field
from psycopg.types.numeric import FloatLoader
from typing_extensions import override

from defio.client import AsyncClient, AsyncConnection
from defio.client.config import DbConfig
from defio.sql.ast.statement import CreateStatement, DropStatement
from defio.sql.parser import parse_sql

_SECONDS_TO_MILLISECONDS: Final = 1000


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
        statement_timeout: timedelta | None = None,
    ) -> PostgresConnection:
        """
        Creates a Postgres async connection with the given connection parameters.

        If `ssl_root_cert_path` is provided, use SSL when connecting to the database.
        This will fail if the database doesn't have the right server certificate
        (i.e. it must be verifiable using the given root CA).

        If `statement_timeout` is provided, override the default value set by
        the database. The given timeout will be rounded to the nearest milliseconds.
        When enabled, this connection will raise an exception if it takes more than
        the specified amount of time to execute a query.

        Raises a `ValueError` if `statement_timeout` is not positive.

        TODO: Custom exceptions
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
            options=(
                "-c statement_timeout="
                f"{int(statement_timeout.total_seconds() * _SECONDS_TO_MILLISECONDS)}"
                if statement_timeout is not None
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

        # Convert `numeric` types to `float` instead of `Decimal`
        # (the latter is not JSON-serializable)
        # https://www.psycopg.org/psycopg3/docs/advanced/adapt.html#example-postgresql-numeric-to-python-float
        aconn.adapters.register_loader("numeric", FloatLoader)

        return PostgresConnection(aconn=aconn)

    @override
    async def close(self) -> None:
        if self._closed:
            raise ValueError("Connection is already closed")

        await self._aconn.close()
        self._closed = True

    @override
    async def execute(self, query: str) -> AsyncIterator[tuple[Any, ...]]:
        try:
            # Psycopg only accepts `LiteralString`, so disable type check here
            cursor = await self._aconn.execute(query)  # type: ignore

        except asyncio.CancelledError:
            # UPDATE: Not sure if the below is needed, since we're reraising
            # the exception anyway (i.e. if a connection is closed, does that
            # cancel all the remaining queries?)

            # Task Group will cancel _all_ children tasks if any error is thrown,
            # so make sure to gracefully cancel the still-running queries
            self._aconn.cancel()
            raise

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

    async def create_tables(
        self, /, *, schema_path: Path, verbose: bool = False
    ) -> None:
        """
        Creates some tables by executing the DDL statements from the given schema file.

        Raises a `ValueError` if the schema cannot be read from the filesystem,
        or if it is not a valid schema (which must consist of only `CREATE` and
        `DROP` statements).
        """
        try:
            with open(schema_path, mode="r", encoding="utf-8") as f:
                ddl_statements = parse_sql(f.read())
        except OSError as exc:
            raise ValueError("Schema file does not exist") from exc
        except ValueError as exc:
            raise ValueError("Schema cannot be parsed") from exc

        # Run statements _sequentially_ in the topological order
        # based on foreign key constraint dependencies
        # (not enforced with code; schema file must obey this)
        async with await self.connect() as aconn:
            for statement in ddl_statements:
                if not isinstance(statement, (CreateStatement, DropStatement)):
                    raise ValueError(
                        "Schema is not valid, as it contains "
                        "statements other than `CREATE` and `DROP`"
                    )

                if verbose:
                    # Truncate `CREATE` statements (which are generally too long)
                    truncated_statement = re.sub(r"\(.+\)", "(...)", str(statement))
                    print(f'Executing: "{truncated_statement}"')

                await aconn.execute_one(str(statement))

        if verbose:
            print("---")

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
    async def connect(
        self, statement_timeout: timedelta | None = None
    ) -> PostgresConnection:
        return await PostgresConnection.create(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            dbname=self.dbname,
            ssl_root_cert_path=self.ssl_root_cert_path,
            statement_timeout=statement_timeout,
        )
