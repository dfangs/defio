import asyncio
import math
from asyncio import PriorityQueue, Queue, Task, TaskGroup
from datetime import timedelta
from typing import Final, Literal, TypeVar, assert_never

from defio.client import AsyncClient
from defio.utils.queue import PrioritizedItem, QueueSignal
from defio.utils.time import get_current_time, get_event_loop_time, measure_time
from defio.workload import Workload
from defio.workload.query import Query, QueryReport, QuerySource, ScheduledQuery, User
from defio.workload.reporter import BlankQueryReporter, QueryReporter
from defio.workload.schedule import Repeat

_T = TypeVar("_T")

# Don't consume the queries all at once
_MAX_SCHEDULED_QUEUE_SIZE: Final[int] = 10


async def run_workload(
    *,
    workload: Workload,
    client: AsyncClient[_T],
    reporter: QueryReporter[_T] = BlankQueryReporter(),
    statement_timeout: timedelta | None = None,
) -> None:
    """
    Runs the given workload asynchronously with the given client and reports
    the completion of each executed query (either success or failure) through
    the given reporter.
    """
    background_tasks = set[Task[None]]()
    completed_queue = Queue[QueryReport[_T] | Literal[QueueSignal.DONE]]()

    async with TaskGroup() as tg:
        for user, query_source in workload:
            scheduled_queue = PriorityQueue[
                PrioritizedItem[ScheduledQuery | Literal[QueueSignal.DONE]]
            ](maxsize=_MAX_SCHEDULED_QUEUE_SIZE)

            # Keep references to the background tasks
            # See https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
            background_tasks |= {
                tg.create_task(
                    _initial_scheduler_worker(user, query_source, scheduled_queue)
                ),
                tg.create_task(
                    _executor_worker(
                        scheduled_queue, completed_queue, client, statement_timeout
                    )
                ),
            }

        background_tasks |= {
            tg.create_task(_reporter_worker(completed_queue, len(workload), reporter))
        }

        # Ensure the removal of the saved references
        for task in background_tasks:
            task.add_done_callback(background_tasks.discard)


async def _process_single_query(
    user: User,
    query: Query,
    scheduled_queue: PriorityQueue[
        PrioritizedItem[ScheduledQuery | Literal[QueueSignal.DONE]]
    ],
) -> None:
    processed_time = get_current_time()

    # Use the event loop time for scheduling purposes since it guarantees monotonicity
    scheduled_internal_time = (
        get_event_loop_time() + query.schedule.time_until_next().total_seconds()
    )

    # NOTE:
    # For queries whose schedule already passed (i.e. negative `time_until_next`),
    # we try to schedule those with an earlier schedule before the others,
    # but the final scheduled time might still be imprecise.
    #
    # For example, if two queries have the same `Once` schedule that has already
    # passed, their order might get swapped due to the different timings of
    # `get_event_loop_time()` and `get_current_time()`.

    await scheduled_queue.put(
        PrioritizedItem(
            scheduled_internal_time,
            query.start(
                user=user,
                processed_time=processed_time,
                scheduled_time=processed_time + query.schedule.time_until_next(),
            ),
        )
    )


async def _initial_scheduler_worker(
    user: User,
    query_source: QuerySource,
    scheduled_queue: PriorityQueue[
        PrioritizedItem[ScheduledQuery | Literal[QueueSignal.DONE]]
    ],
) -> None:
    """Worker for scheduling queries from the given query source."""
    for query in query_source:
        # All queries are guaranteed to be executed at least once
        await _process_single_query(user, query, scheduled_queue)

    await scheduled_queue.put(PrioritizedItem(math.inf, QueueSignal.DONE))


async def _executor_worker(
    scheduled_queue: PriorityQueue[
        PrioritizedItem[ScheduledQuery | Literal[QueueSignal.DONE]]
    ],
    completed_queue: Queue[QueryReport[_T] | Literal[QueueSignal.DONE]],
    client: AsyncClient[_T],
    statement_timeout: timedelta | None,
) -> None:
    """Worker for executing scheduled queries."""
    while True:
        queue_item = await scheduled_queue.get()
        match queue_item.item:
            case ScheduledQuery():
                scheduled_internal_time = queue_item.priority
                scheduled_query = queue_item.item

            case QueueSignal.DONE:
                await completed_queue.put(QueueSignal.DONE)
                return

            case _:
                assert_never(queue_item.item)

        # Wait for some time if no query is scheduled right now
        current_event_loop_time = get_event_loop_time()
        if current_event_loop_time < scheduled_internal_time:
            await asyncio.sleep(scheduled_internal_time - current_event_loop_time)

        results, error = None, None

        # Execute the scheduled query once
        with measure_time() as measurement:
            try:
                async with await client.connect(
                    statement_timeout=statement_timeout
                ) as aconn:
                    results = [
                        row async for row in aconn.execute(scheduled_query.query.sql)
                    ]

            except Exception as exc:  # pylint: disable=broad-exception-caught
                # TODO: Specialize the exceptions
                error = exc

        await completed_queue.put(
            scheduled_query.create_report(
                executed_time=measurement.start_time,
                execution_time=measurement.elapsed_time,
                results=results,
                error=error,
            )
        )

        scheduled_queue.task_done()

        # Check if the query should be re-scheduled
        if (
            isinstance(schedule := scheduled_query.query.schedule, Repeat)
            and schedule.time_until_next().total_seconds() >= 0
        ):
            await _process_single_query(
                scheduled_query.user, scheduled_query.query, scheduled_queue
            )


async def _reporter_worker(
    completed_queue: Queue[QueryReport[_T] | Literal[QueueSignal.DONE]],
    num_executor_workers: int,
    reporter: QueryReporter[_T],
) -> None:
    """Worker for reporting query completions."""
    try:
        num_executor_workers_done = 0

        while True:
            match queue_item := await completed_queue.get():
                case QueryReport() as query_report:
                    await reporter.report(query_report)
                    completed_queue.task_done()

                case QueueSignal.DONE:
                    num_executor_workers_done += 1
                    completed_queue.task_done()

                    # Finish only when all executors have finished their jobs
                    if num_executor_workers_done == num_executor_workers:
                        await reporter.done()
                        return

                case _:
                    assert_never(queue_item)

    except asyncio.CancelledError:
        # In case of cancellation (e.g., by Task Group),
        # make sure to report the remaining items
        while not completed_queue.empty():
            query_report = completed_queue.get_nowait()

            # No need to handle special signals anymore
            if query_report is QueueSignal.DONE:
                continue

            await reporter.report(query_report)

        await reporter.done()
        raise
