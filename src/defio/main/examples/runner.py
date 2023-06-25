import asyncio
from datetime import timedelta
from typing import Any, AsyncIterator, final

from typing_extensions import override

from defio.client import AsyncClient, AsyncConnection
from defio.utils.time import get_current_time
from defio.workload import Workload
from defio.workload.query import Query, QueryReport
from defio.workload.reporter import QueryReporter
from defio.workload.runner import run_workload
from defio.workload.schedule import Once, Repeat
from defio.workload.user import User


@final
class NoopConnection(AsyncConnection[str]):
    @override
    async def close(self) -> None:
        pass

    @override
    async def execute(self, query: str) -> AsyncIterator[str]:
        yield ""

    @override
    async def transaction(self) -> AsyncIterator[None]:
        raise NotImplementedError


@final
class NoopClient(AsyncClient[str]):
    @override
    async def connect(
        self, statement_timeout: timedelta | None = None
    ) -> AsyncConnection[str]:
        return NoopConnection()


@final
class SimpleQueryReporter(QueryReporter[Any]):
    """Simple query reporter for demonstration and testing purposes."""

    @override
    async def report(self, query_report: QueryReport[Any]) -> None:
        print(
            f"[{query_report.completed_time}] "
            f"{query_report.user.label}: "
            f"{query_report.query.sql}"
        )

    @override
    async def done(self) -> None:
        print("Finished running the workload")


async def main() -> None:
    current_time = get_current_time()
    interval = timedelta(seconds=1)

    workload = Workload.combine(
        [
            Workload.serial(
                [
                    Query(f"SELECT {i};", Once(at=current_time + i * 0.47 * interval))
                    for i in range(10)
                ],
                user=User.with_label("Once"),
            ),
            Workload.serial(
                [
                    Query(
                        "SELECT *;",
                        Repeat.starting_now(interval=interval, num_repeat=20),
                    ),
                ],
                user=User.with_label("Repeat"),
            ),
        ]
    )

    await run_workload(
        workload=workload, client=NoopClient(), reporter=SimpleQueryReporter()
    )


if __name__ == "__main__":
    asyncio.run(main())
