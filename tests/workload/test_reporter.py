import tempfile
from collections.abc import Sequence
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Final

import pytest

from defio.utils.time import get_current_time
from defio.workload.query import Query, QueryReport
from defio.workload.reporter import FileQueryReporter, SimpleQueryReport
from defio.workload.schedule import Once
from defio.workload.user import User

NUM_REPORTS: Final = 10


class TestMinimumQueryReport:
    def test_serde(self) -> None:
        report = SimpleQueryReport(
            sql="SELECT 1;",
            execution_time=timedelta(seconds=3.14),
            results=[(1, "two", 3.0), (4, "five", 6.0)],
        )

        assert SimpleQueryReport.loads(report.dumps()) == report

    def test_load(self) -> None:
        reports = [
            SimpleQueryReport(
                sql=f"SELECT {i};",
                execution_time=timedelta(seconds=3.14 + i * 0.1),
                results=([(i,)] if i % 2 == 0 else None),
                error_msg=(f"timeout: {i}" if i % 2 == 1 else None),
            )
            for i in range(NUM_REPORTS)
        ]

        stream = StringIO()
        for report in reports:
            stream.write(report.dumps() + "\n")

        stream.seek(0)
        assert SimpleQueryReport.load_all(stream) == reports


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
                results=([(i,)] if i % 2 == 0 else None),
                error=(TimeoutError("timeout") if i % 2 == 1 else None),
            )
            for i in range(NUM_REPORTS)
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
            reporter = FileQueryReporter(dirpath, "test")

            for query_report in query_reports:
                await reporter.report(query_report)

            if done:
                await reporter.done()

            filepaths = list(dirpath.iterdir())
            assert len(filepaths) == 1

            report_path = next(iter(filepaths))
            assert "".join(report_path.suffixes) == expected_suffix

            with open(report_path, mode="r", encoding="utf-8") as f:
                reports = SimpleQueryReport.load_all(f)

            assert reports == [
                SimpleQueryReport(
                    sql=query_report.query.sql,
                    execution_time=query_report.execution_time,
                    results=query_report.results,
                    error_msg=(
                        str(query_report.error)
                        if query_report.error is not None
                        else None
                    ),
                )
                for query_report in query_reports
            ]

    @pytest.mark.asyncio
    async def test_done_without_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            dirpath = Path(tmpdirname)
            reporter = FileQueryReporter(dirpath, "test")

            await reporter.done()

            assert len(list(dirpath.iterdir())) == 0
