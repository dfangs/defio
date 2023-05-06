from abc import abstractmethod
from typing import Any, Protocol, TypeVar, final

from typing_extensions import override

from defio.workload.query import QueryReport

_T = TypeVar("_T")


class QueryReporter(Protocol[_T]):
    """Protocol for reporting query completions."""

    @abstractmethod
    async def report(self, query_report: QueryReport[_T]) -> None:
        """Reports the completion of the given query."""
        raise NotImplementedError

    @abstractmethod
    async def done(self) -> None:
        """
        Signals the successful completion of the workload run.
        """
        raise NotImplementedError

    @abstractmethod
    async def error(self, exc: BaseException) -> None:
        """
        Signals the premature end of the workload run due to some
        exception being raised.
        """
        raise NotImplementedError


@final
class BlankQueryReporter(QueryReporter[Any]):
    """No-op query reporter."""

    @override
    async def report(self, query_report: QueryReport[Any]) -> None:
        ...

    @override
    async def done(self) -> None:
        ...

    @override
    async def error(self, exc: BaseException) -> None:
        ...


@final
class SimpleQueryReporter(QueryReporter[Any]):
    """Simple query reporter for demonstration and testing purposes."""

    @override
    async def report(self, query_report: QueryReport) -> None:
        print(
            f"[{query_report.completed_time}] "
            f"{query_report.user.label}: "
            f"{query_report.query.sql}"
        )

    @override
    async def done(self) -> None:
        print("Finished running the workload")

    @override
    async def error(self, exc: Exception | None = None) -> None:
        print(f"Exception `{exc}` occurred during the workload run")
