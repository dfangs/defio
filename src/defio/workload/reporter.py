from __future__ import annotations

import json
import traceback
from abc import abstractmethod
from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, TextIO, TypeVar, final

from attrs import define, field
from typing_extensions import override

from defio.utils.attrs import to_tuple
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


@define(frozen=True, kw_only=True)
class MinimumQueryReport:
    """
    Minimum version of `QueryReport` specialized for PostgreSQL queries.

    It only contains the SQL string, execution time, and the result tuples
    of the query, since it is intended to be used only for training the
    cost model of the query router.
    """

    sql: str
    execution_time: timedelta
    results: Sequence[tuple[Any, ...]] = field(converter=to_tuple)

    @staticmethod
    def from_str(input_str: str) -> MinimumQueryReport:
        """Converts the given string into a minimum query report."""
        json_dict = json.loads(input_str)
        return MinimumQueryReport(
            sql=json_dict["sql"],
            execution_time=timedelta(seconds=json_dict["execution_time"]),
            results=tuple(tuple(result_tuple) for result_tuple in json_dict["results"]),
        )

    def to_str(self) -> str:
        """Converts this minimum query report into a single-line string."""
        return json.dumps(
            {
                "sql": self.sql,
                "execution_time": self.execution_time.total_seconds(),
                "results": self.results,
            }
        )

    @staticmethod
    def load(f: TextIO) -> Sequence[MinimumQueryReport]:
        """Loads the given file into a list of minimum query reports."""
        return [MinimumQueryReport.from_str(line) for line in f]


@final
@define(frozen=True)
class FileQueryReporter(QueryReporter[tuple[Any, ...]]):
    """
    Query reporter that writes `MinimumQueryReport` to the filesystem.

    Mainly used for collecting execution data for training the cost model
    of the query router.

    Report files are identified by the time at which the reporter is created.
    """

    directory: Path
    _creation_time: datetime = field(init=False, factory=datetime.now)

    def __attrs_post_init__(self) -> None:
        # Create directory if it doesn't exist
        self.directory.mkdir(parents=True, exist_ok=True)

    @override
    async def report(self, query_report: QueryReport[tuple[Any, ...]]) -> None:
        report_line = MinimumQueryReport(
            sql=query_report.query.sql,
            execution_time=query_report.execution_time,
            results=query_report.results,
        ).to_str()

        self._write_report(report_line)

    @override
    async def done(self) -> None:
        self._finalize_report()

    @override
    async def error(self, exc: BaseException) -> None:
        # Write the exception to a separate file
        error_path = self.directory / f"{self._report_name}.error.txt"
        with open(error_path, mode="w", encoding="utf-8") as f:
            traceback.print_exception(exc, file=f)

        self._finalize_report()

    @property
    def _report_name(self) -> str:
        timestamp = self._creation_time.strftime("%Y%m%d-%H%M%S")
        return f"report-{timestamp}"

    @property
    def _report_path(self) -> Path:
        return self.directory / f"{self._report_name}.temp.txt"

    def _write_report(self, line: str) -> None:
        with open(self._report_path, mode="a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _finalize_report(self) -> None:
        # No report files are created if there were no queries to report
        # (i.e. the file is created at the first call to `report()`)
        if not self._report_path.exists():
            return

        # Remove the `.temp` infix
        self._report_path.rename(self.directory / f"{self._report_name}.txt")
