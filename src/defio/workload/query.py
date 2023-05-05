from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta
from typing import Any, Generic, TypeAlias, TypeVar, final

from attrs import define, field
from typing_extensions import override

from defio.sqlgen.generator import SqlGenerator
from defio.utils.generator import ImmutableGenerator
from defio.utils.time import get_current_time
from defio.workload.schedule import Once, Schedule
from defio.workload.user import User


@final
@define(frozen=True)
class Query:
    """
    Represents a SQL query with an execution schedule.

    In the context of a workload runner, a query goes through
    the following lifecycle:
    - `INITIAL`: The query has not been processed by the runner yet.
    - `STARTED`: The query has started being processed the runner.
    - `SCHEDULED`: The query has been scheduled to run by the runner.
    - `RUNNING`: The query has been executed on some database engine.
    - `COMPLETED`: The query has completed its execution schedule.

    Note that for a recurring query (i.e. query with a `Repeat` schedule),
    it only reaches the `COMPLETED` state after the entire execution
    schedule has completed. During its execution period, its state will
    switch back and forth between `SCHEDULED` and `RUNNING`.
    """

    sql: str
    schedule: Schedule

    def start(
        self, /, *, user: User, processed_time: datetime, scheduled_time: datetime
    ) -> ScheduledQuery:
        """Schedules this query to be executed at least once in the future."""
        return ScheduledQuery(
            user=user,
            query=self,
            processed_time=processed_time,
            scheduled_time=scheduled_time,
        )


@final
@define(frozen=True, kw_only=True)
class ScheduledQuery:
    """
    Represents a user query that has been scheduled by a workload runner
    to run _once_ at some time in the future (i.e. it is in the `SCHEDULED`
    state).

    A single `Query` can be scheduled multiple times by the same user
    (e.g., with a `Repeat` schedule) or different users.

    Definitions:
    - `processed_time` is the time when the query transitions from
      some other state (e.g., `STARTED` or `RUNNING`) to `SCHEDULED`.
    - `scheduled_time` is the time when the query is scheduled to
      be executed on some database.
    """

    user: User
    query: Query
    processed_time: datetime
    scheduled_time: datetime

    def create_report(
        self,
        /,
        *,
        executed_time: datetime,
        execution_time: timedelta,
        results: Sequence[Any],
    ) -> QueryReport:
        """Converts this query into a `CompletedQuery`."""
        return QueryReport(
            user=self.user,
            query=self.query,
            processed_time=self.processed_time,
            scheduled_time=self.scheduled_time,
            executed_time=executed_time,
            execution_time=execution_time,
            results=results,
        )


_T = TypeVar("_T")


@final
@define(frozen=True, eq=False, kw_only=True)
class QueryReport(Generic[_T]):
    """
    Represents a single execution of a scheduled user query.
    This contains some information useful for reporting purposes.

    Definitions:
    - `executed_time` is the time when the query transitions from
      `SCHEDULED` to `RUNNING`.
    - `completed_time` is the time when the query transitions from
      `RUNNING` to `COMPLETED`.
    - `execution_time` is the duration from `executed_time` to
      `completed_time`.
    """

    user: User
    query: Query
    processed_time: datetime
    scheduled_time: datetime
    executed_time: datetime
    execution_time: timedelta
    results: Sequence[_T]

    @property
    def completed_time(self) -> datetime:
        return self.executed_time + self.execution_time


# Avoid using `Iterable[Query]` since it complicates immutable design.
# Specifically, we can't simply defensive copy all iterables--what if they're unbounded?
QuerySource: TypeAlias = Sequence[Query] | ImmutableGenerator[Query]


@final
@define(frozen=True)
class QueryGenerator(ImmutableGenerator[Query]):
    """
    Immutable generator that yields a (potentially unbounded) sequence
    of queries.

    This class is intended to convert a `SqlGenerator` (which yields SQL
    strings) into a generator of full-fledged `Query` objects. As such,
    the attached `Schedule` should only be instances of `Once`, since
    `SqlGenerator` shouldn't be used to generate recurring queries.
    """

    _sql_generator: SqlGenerator = field(alias="sql_generator")
    _fixed_time: Once | None = field(default=None, alias="fixed_time")
    _interval: timedelta | None = field(default=None, alias="interval")

    def __attrs_post_init__(self) -> None:
        assert sum([self._fixed_time is None, self._interval is None]) == 1

    @override
    def __iter__(self) -> Iterator[Query]:
        if self._fixed_time is not None:
            for sql in self._sql_generator:
                yield Query(sql, self._fixed_time)
            return

        assert self._interval is not None

        start_time = get_current_time()
        for i, sql in enumerate(self._sql_generator):
            yield Query(sql, Once(start_time + i * self._interval))

    @staticmethod
    def with_fixed_time(sql_generator: SqlGenerator, schedule: Once) -> QueryGenerator:
        """
        Creates a query generator that yields queries with the given
        fixed schedule.
        """
        return QueryGenerator(sql_generator=sql_generator, fixed_time=schedule)

    @staticmethod
    def with_fixed_interval(
        sql_generator: SqlGenerator, interval: timedelta
    ) -> QueryGenerator:
        """
        Creates a query generator that yields queries scheduled at
        evenly-spaced interval starting from the first yield.
        """
        return QueryGenerator(sql_generator=sql_generator, interval=interval)

    @staticmethod
    def with_fixed_rate(
        sql_generator: SqlGenerator, queries_per_second: float
    ) -> QueryGenerator:
        """
        Creates a query generator that yields queries with evenly-spaced
        schedules starting from the first yield, such that no more than
        `queries_per_second` queries are scheduled within a one-second interval.
        """
        return QueryGenerator(
            sql_generator=sql_generator,
            interval=timedelta(seconds=1) / queries_per_second,
        )
