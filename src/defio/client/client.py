from abc import abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from types import TracebackType
from typing import Protocol, Self, TypeVar, final

_Row_co = TypeVar("_Row_co", covariant=True)


class AsyncConnection(Protocol[_Row_co]):
    """
    Represents an asynchronous connection to a database.

    Each individual connection can only perform one operation at a time,
    i.e. it is not suitable for achieving parallelism. Instead, create
    multiple connections to execute concurrent queries.

    This class should be used as a context manager:
    ```
    async with AsyncConnection(...) as aconn:
        # Single statement
        async for row in aconn.execute(...):
            ...

        # Multiple statements in a transaction
        async with aconn.transaction():
            await aconn.execute_one(...)
            await aconn.execute_one(...)
    ```
    """

    @final
    async def __aenter__(self) -> Self:
        return self

    @final
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        # Ignore the arguments, i.e. no need to handle/suppress exception
        await self.close()

    @abstractmethod
    async def close(self) -> None:
        """
        Closes this async connection.

        Note that this does not automatically commit or rollback
        any transactions.
        """
        raise NotImplementedError

    @abstractmethod
    async def execute(self, query: str) -> AsyncIterator[_Row_co]:
        """
        Executes the given SQL query/command outside of a transaction
        and yields the resulting tuples.
        """
        # Use `yield` keyword to let the type checker know it is an async generator
        # See https://stackoverflow.com/a/68911014/21451742
        yield  # type: ignore
        raise NotImplementedError

    @final
    async def execute_one(self, query: str) -> _Row_co | None:
        """
        Executes the given SQL query/command outside of a transaction
        and returns only the first tuple (if it exists).

        This is especially useful for executing statements that
        we don't expect to return a value (e.g., `CREATE` statements).
        """
        async for row in self.execute(query):
            return row

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """
        Enters a transaction block.

        All calls to `execute()` within this block will be executed
        in a single transaction.
        """
        yield  # type: ignore
        raise NotImplementedError("Transactions are not supported")


class AsyncClient(Protocol[_Row_co]):
    """
    Represents an asynchronous client interface for a database.

    A client can be used to create async connections, each representing
    a single database session. If concurrent execution is desired,
    create multiple connections instead of sharing the same connection.

    Usage:
    ```
    client = AsyncClient(...)
    async with await client.connect(...) as aconn:
        ...
    ```
    """

    @abstractmethod
    async def connect(
        self, statement_timeout: timedelta | None = None
    ) -> AsyncConnection[_Row_co]:
        """
        Creates an async connection to the database this client is
        interfacing with.

        If `statement_timeout` is provided, the implementing connection
        class should abort (i.e. raise an exception) if it takes more
        than the specified amount of time to execute a query.
        """
        raise NotImplementedError
