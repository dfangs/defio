from collections.abc import Iterator
from datetime import datetime, timedelta
from itertools import pairwise, repeat
from typing import Final, final

from attrs import define

from defio.sqlgen.generator import SqlGenerator
from defio.workload.query import Query, QueryGenerator
from defio.workload.schedule import Once
from defio.workload.user import User


def test_query() -> None:
    query = Query(
        sql := "SELECT 1;",
        schedule := Once(at=datetime(year=2023, month=5, day=2)),
    )

    assert query.sql == sql
    assert query.schedule == schedule

    scheduled_query = query.start(
        user=(user := User.random()),
        processed_time=(processed_time := schedule.at - timedelta(seconds=1)),
        scheduled_time=(scheduled_time := schedule.at + timedelta(seconds=0.1)),
    )

    assert scheduled_query.user == user
    assert scheduled_query.query == query
    assert scheduled_query.processed_time == processed_time
    assert scheduled_query.scheduled_time == scheduled_time

    query_report = scheduled_query.create_report(
        executed_time=(executed_time := scheduled_time + timedelta(seconds=0.1)),
        execution_time=(execution_time := timedelta(milliseconds=10)),
        results=[1],
    )

    assert query_report.user == user
    assert query_report.query == query
    assert query_report.processed_time == processed_time
    assert query_report.scheduled_time == scheduled_time
    assert query_report.executed_time == executed_time
    assert query_report.execution_time == execution_time
    assert query_report.completed_time == executed_time + execution_time


SQL_TEMPLATE = "SELECT {i};"
NUM_ITERS: Final = 3


@final
@define(frozen=True)
class DummySqlGenerator(SqlGenerator):
    num_items: int

    def __iter__(self) -> Iterator[str]:
        for i in range(self.num_items):
            yield SQL_TEMPLATE.format(i=i)


class TestQueryGenerator:
    def test_fixed_time(self) -> None:
        sql_list = list(DummySqlGenerator(num_items := 10))
        schedule = Once(at=datetime(year=2023, month=5, day=2))

        query_generator = QueryGenerator.with_fixed_time(sql_list, schedule)

        # Query generators can be iterated multiple times
        for _ in range(NUM_ITERS):
            assert list(query_generator) == [
                Query(SQL_TEMPLATE.format(i=i), schedule) for i in range(num_items)
            ]

    def test_fixed_interval(self) -> None:
        sql_generator = DummySqlGenerator(num_items := 10)
        interval = timedelta(seconds=0.5)

        query_generator = QueryGenerator.with_fixed_interval(sql_generator, interval)

        # Query generators can be iterated multiple times
        for _ in range(NUM_ITERS):
            TestQueryGenerator._assert_intervals(
                list(query_generator), num_items, interval
            )

    def test_fixed_rate(self) -> None:
        sql_generator = DummySqlGenerator(num_items := 10)
        interval = timedelta(seconds=1) / (queries_per_second := 2)

        query_generator = QueryGenerator.with_fixed_rate(
            sql_generator, queries_per_second
        )

        # Query generators can be iterated multiple times
        for _ in range(NUM_ITERS):
            TestQueryGenerator._assert_intervals(
                list(query_generator), num_items, interval
            )

    @staticmethod
    def _assert_intervals(
        queries: list[Query], num_items: int, interval: timedelta
    ) -> None:
        assert [query.sql for query in queries] == [
            SQL_TEMPLATE.format(i=i) for i in range(num_items)
        ]

        schedules: list[Once] = []
        for query in queries:
            assert isinstance(query.schedule, Once)
            schedules.append(query.schedule)

        intervals = list(after.at - before.at for before, after in pairwise(schedules))

        assert intervals == list(repeat(interval, num_items - 1))
