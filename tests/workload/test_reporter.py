import asyncio
import tempfile
from collections.abc import Sequence
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import pytest

from defio.utils.time import get_current_time
from defio.workload.query import Query, QueryReport
from defio.workload.reporter import FileQueryReporter, MinimumQueryReport
from defio.workload.schedule import Once
from defio.workload.user import User


class TestMinimumQueryReport:
    def test_serde(self) -> None:
        report = MinimumQueryReport(
            sql="SELECT 1;",
            execution_time=timedelta(seconds=3.14),
            results=[(1, "two", 3.0), (4, "five", 6.0)],
        )

        assert MinimumQueryReport.from_str(report.to_str()) == report

    def test_load(self) -> None:
        num_reports = 10
        reports = [
            MinimumQueryReport(
                sql=f"SELECT {i};",
                execution_time=timedelta(seconds=3.14 + i * 0.1),
                results=[(i * 1, i * 2.0)],
            )
            for i in range(num_reports)
        ]

        stream = StringIO()
        for report in reports:
            stream.write(report.to_str() + "\n")

        stream.seek(0)
        assert MinimumQueryReport.load(stream) == reports


class TestFileQueryReporter:
    @pytest.fixture(name="query_reports")
    def fixture_query_reports(self) -> Sequence[QueryReport[tuple[Any, ...]]]:
        return [
            QueryReport(
                user=User.random(),
                query=Query(sql=f"SELECT {i};", schedule=Once.now()),
                processed_time=get_current_time(),
                scheduled_time=get_current_time(),
                executed_time=get_current_time(),
                execution_time=timedelta(seconds=3.14 + i * 0.1),
                results=[(i * 1, i * 2.0)],
            )
            for i in range(3)
        ]

    @pytest.mark.parametrize(
        "done, expected_suffix",
        [
            (False, ".temp.txt"),
            (True, ".txt"),
        ],
    )
    @pytest.mark.asyncio
    async def test_report(
        self,
        query_reports: Sequence[QueryReport[tuple[Any, ...]]],
        done: bool,
        expected_suffix: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            dirpath = Path(tmpdirname)
            reporter = FileQueryReporter(dirpath)

            for query_report in query_reports:
                await reporter.report(query_report)

            if done:
                await reporter.done()

            filepaths = list(dirpath.iterdir())
            assert len(filepaths) == 1

            report_path = next(iter(filepaths))
            assert "".join(report_path.suffixes) == expected_suffix

            TestFileQueryReporter._assert_report_file(report_path, query_reports)

    @pytest.mark.asyncio
    async def test_error(
        self, query_reports: Sequence[QueryReport[tuple[Any, ...]]]
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            dirpath = Path(tmpdirname)
            reporter = FileQueryReporter(dirpath)

            for query_report in query_reports:
                await reporter.report(query_report)

            await reporter.error(asyncio.CancelledError())

            filepaths = list(dirpath.iterdir())
            report_path = get_file_with_suffix(filepaths, ".txt")
            error_path = get_file_with_suffix(filepaths, ".error.txt")

            # Both the report and the error file must exist
            assert len(filepaths) == 2
            assert report_path is not None
            assert error_path is not None

            # NOTE: Can't exactly test the content of the error file
            TestFileQueryReporter._assert_report_file(report_path, query_reports)

    @pytest.mark.asyncio
    async def test_done_without_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            dirpath = Path(tmpdirname)
            reporter = FileQueryReporter(dirpath)

            await reporter.done()

            assert len(list(dirpath.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_error_without_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            dirpath = Path(tmpdirname)
            reporter = FileQueryReporter(dirpath)

            await reporter.error(asyncio.CancelledError())

            filepaths = list(dirpath.iterdir())
            assert len(filepaths) == 1

            error_path = next(iter(filepaths))
            assert "".join(error_path.suffixes) == ".error.txt"

    @staticmethod
    def _assert_report_file(
        report_path: Path, query_reports: Sequence[QueryReport[tuple[Any, ...]]]
    ) -> None:
        with open(report_path, mode="r", encoding="utf-8") as f:
            reports = MinimumQueryReport.load(f)

        assert reports == [
            MinimumQueryReport(
                sql=query_report.query.sql,
                execution_time=query_report.execution_time,
                results=query_report.results,
            )
            for query_report in query_reports
        ]


def get_file_with_suffix(paths: Sequence[Path], suffix: str) -> Path | None:
    return next(
        (path for path in paths if "".join(path.suffixes) == suffix),
        None,
    )
