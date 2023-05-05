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
        Signals the end of a workload run, either due to all queries
        have been processed or some exception was raised.
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
