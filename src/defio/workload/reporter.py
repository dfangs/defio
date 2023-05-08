from __future__ import annotations

import json
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
        """
        Reports the completion (either success or failure) of the given query.
        """
        raise NotImplementedError

    @abstractmethod
    async def done(self) -> None:
        """
        Reports the completion of the workload run.

        Raises a `ValueError` if `done()` has already been called previously.
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
@define(frozen=True, kw_only=True)
class SimpleQueryReport:
    """
    Simple version of `QueryReport` specialized for PostgreSQL queries.

    It only contains the SQL string, execution time, and either the
    result tuples of the query (if the query succeeded) or the error
    message (if the query failed), since it is intended to be used
    only for training the cost model of the query router.
    """

    sql: str
    execution_time: timedelta
    results: Sequence[tuple[Any, ...]] | None = field(default=None, converter=to_tuple)
    error_msg: str | None = None

    def __attrs_post_init__(self) -> None:
        assert (self.results is not None) ^ (self.error_msg is not None)

    @staticmethod
    def from_dict(json_dict: dict[str, Any]) -> SimpleQueryReport:
        """Converts the given JSON dict into a simple query report."""
        return SimpleQueryReport(
            sql=json_dict["sql"],
            execution_time=timedelta(seconds=json_dict["execution_time"]),
            results=(
                tuple(tuple(result_tuple) for result_tuple in json_dict["results"])
                if json_dict["results"] is not None
                else None
            ),
            error_msg=(
                json_dict["error_msg"] if json_dict["error_msg"] is not None else None
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Converts this simple query report into a JSON dict."""
        return {
            "sql": self.sql,
            "execution_time": self.execution_time.total_seconds(),
            "results": self.results,
            "error_msg": self.error_msg,
        }

    @staticmethod
    def loads(input_str: str) -> SimpleQueryReport:
        """Converts the given string into a minimum query report."""
        return SimpleQueryReport.from_dict(json.loads(input_str))

    def dumps(self) -> str:
        """Converts this minimum query report into a single-line string."""
        return json.dumps(self.to_dict())

    @staticmethod
    def load_all(f: TextIO) -> Sequence[SimpleQueryReport]:
        """Loads the given text stream into a list of minimum query reports."""
        return [SimpleQueryReport.loads(line) for line in f]


@final
@define(frozen=True)
class FileQueryReporter(QueryReporter[tuple[Any, ...]]):
    """
    Query reporter that writes `ShortQueryReport` to the filesystem.

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
        report_line = SimpleQueryReport(
            sql=query_report.query.sql,
            execution_time=query_report.execution_time,
            results=query_report.results,
            error_msg=(
                str(query_report.error) if query_report.error is not None else None
            ),
        ).dumps()

        self._write_report(report_line)

    @override
    async def done(self) -> None:
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
