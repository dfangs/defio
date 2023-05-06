import asyncio
import re
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from itertools import pairwise
from typing import Final, cast, final

import pytest
from attrs import define, field
from typing_extensions import override

from defio.client import AsyncClient, AsyncConnection
from defio.workload import Workload
from defio.workload.query import Query, QueryReport
from defio.workload.reporter import QueryReporter
from defio.workload.runner import run_workload
from defio.workload.schedule import Once, Repeat
from defio.workload.user import User

SELECT_QUERY = "SELECT {i};"
SELECT_QUERY_REGEX = re.compile(r"SELECT (\d+);")

TIMEOUT_SECONDS: Final = 1
NUM_QUERIES = 10


@final
@define(frozen=True)
class SpyConnection(AsyncConnection):
    slow: bool

    @override
    async def close(self) -> None:
        ...

    @override
    async def execute(self, query: str) -> AsyncIterator[int]:
        if self.slow:
            await asyncio.sleep(TIMEOUT_SECONDS)

        match = SELECT_QUERY_REGEX.fullmatch(query)
        assert match is not None

        yield int(match.group(1))


@final
@define
class SpyClient(AsyncClient[int]):
    max_num_connections: int | None = None
    slow: bool = False

    is_maxed: asyncio.Event = field(factory=asyncio.Event)
    _connection_count: int = field(default=0, init=False)

    @override
    async def connect(self) -> AsyncConnection[int]:
        if (
            self.max_num_connections is not None
            and self._connection_count >= self.max_num_connections
        ):
            self.is_maxed.set()
            await asyncio.sleep(TIMEOUT_SECONDS)  # Block
            raise RuntimeError("Test should not reach here")

        self._connection_count += 1
        return SpyConnection(slow=self.slow)


@final
@define
class SpyQueryReporter(QueryReporter[int]):
    reports_by_user: dict[User, list[QueryReport[int]]] = field(factory=dict)
    is_done: asyncio.Event = field(factory=asyncio.Event)
    is_error: asyncio.Event = field(factory=asyncio.Event)

    @override
    async def report(self, query_report: QueryReport[int]) -> None:
        self.reports_by_user.setdefault(query_report.user, []).append(query_report)

    @override
    async def done(self) -> None:
        self.is_done.set()

    @override
    async def error(self, exc: BaseException) -> None:
        self.is_error.set()


@pytest.mark.asyncio
async def test_serial_order() -> None:
    mixed_query_source = [
        Query(
            SELECT_QUERY.format(i=i),
            (
                # Alternate between `Once` and `Repeat`
                Once(datetime(year=2023, month=5, day=2) + i * timedelta(seconds=1))
                if i % 2 == 0
                else Repeat.starting_now(
                    interval=timedelta(milliseconds=1), num_repeat=1
                )
            ),
        )
        for i in range(NUM_QUERIES)
    ]

    async with asyncio.timeout(TIMEOUT_SECONDS):
        await run_workload(
            workload=(
                workload := Workload.combine(
                    Workload.serial(mixed_query_source) for _ in range(3)
                )
            ),
            client=SpyClient(),
            reporter=(reporter := SpyQueryReporter()),
        )

        assert reporter.is_done.is_set()

    # Only check queries with `Once` schedules
    actual_query_execution_orders = {
        user: [
            query_report.query
            for query_report in query_reports
            if isinstance(query_report.query.schedule, Once)
        ]
        for user, query_reports in reporter.reports_by_user.items()
    }

    # Since the original schedules are all in the past, they must be executed in order
    expected_query_execution_orders = {
        user: list(
            sorted(
                (query for query in query_source if isinstance(query.schedule, Once)),
                key=lambda q: cast(Once, q.schedule).at,
            )
        )
        for user, query_source in workload
    }

    assert actual_query_execution_orders == expected_query_execution_orders


@pytest.mark.asyncio
async def test_concurrent() -> None:
    query_source = [
        Query(
            SELECT_QUERY.format(i=0),
            Once(datetime(year=2023, month=5, day=2)),
        )
    ]

    task = asyncio.create_task(
        run_workload(
            workload=Workload.concurrent(
                [query_source for _ in range(2 * NUM_QUERIES)]
            ),
            client=(client := SpyClient(max_num_connections=NUM_QUERIES, slow=True)),
            reporter=(reporter := SpyQueryReporter()),
        )
    )

    async with asyncio.timeout(TIMEOUT_SECONDS):
        # Even if the connections are slow, concurrent queries must not block
        await client.is_maxed.wait()

        # Need to cancel this so that pytest doesn't give a warning
        task.cancel()
        await reporter.is_error.wait()


@pytest.mark.asyncio
async def test_unbounded_repeat() -> None:
    query_source = [
        Query(
            SELECT_QUERY.format(i=0),
            Repeat.starting_now(
                interval=(repeat_interval := timedelta(milliseconds=5))
            ),
        )
    ]

    task = asyncio.create_task(
        run_workload(
            workload=Workload.serial(query_source),
            client=(client := SpyClient(max_num_connections=3 * NUM_QUERIES)),
            reporter=(reporter := SpyQueryReporter()),
        )
    )

    async with asyncio.timeout(TIMEOUT_SECONDS):
        # Since the query is recurring infinitely, this must resolve
        await client.is_maxed.wait()

        # Need to cancel this so that pytest doesn't give a warning
        task.cancel()
        await reporter.is_error.wait()

    assert len(reporter.reports_by_user) == 1
    _, query_reports = reporter.reports_by_user.popitem()

    # Runner should not execute queries before their scheduled times
    assert all(
        query_report.executed_time >= query_report.scheduled_time
        for query_report in query_reports
    )

    scheduled_time_intervals = [
        interval
        for interval in (
            after - before
            for before, after in pairwise(
                query_report.scheduled_time for query_report in query_reports
            )
        )
    ]

    # Under normal execution, queries should be scheduled on time (within some margin)
    error_margin = 0.01
    assert all(
        abs(interval - repeat_interval) <= error_margin * repeat_interval
        for interval in scheduled_time_intervals
    )
