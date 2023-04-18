from abc import abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
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
            aconn.execute(...)
            aconn.execute(...)
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
        Executes a single SQL statement outside of a transaction.
        """
        # Use `yield` keyword to let the type checker know it is an async generator
        # See https://stackoverflow.com/a/68911014/21451742
        yield  # type: ignore
        raise NotImplementedError

    @abstractmethod
    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """
        Enters a transaction block.

        All calls to `execute()` within this block will be executed
        in a single transaction.
        """
        yield  # type: ignore
        raise NotImplementedError


class AsyncClient(Protocol[_Row_co]):
    """
    Represents an asynchronous client interface for a database.

    A client can be used to create async connections, each representing
    a single database session. If concurrent execution is desired,
    create multiple connections instead of sharing the same connection.

    Usage:
    ```
    client = AsyncClient(...)
    async with client.connect(...) as aconn:
        ...
    ```
    """

    @abstractmethod
    async def connect(self) -> AsyncConnection:
        """
        Creates an async connection to the database this client is
        interfacing with.
        """
        raise NotImplementedError
