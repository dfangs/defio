from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import final

from attrs import define
from immutables import Map

from defio.utils.attrs import to_tuple
from defio.utils.generator import chain
from defio.workload.query import QuerySource
from defio.workload.user import User


@final
@define(frozen=True)
class Workload:
    """
    Represents an immutable workload that can be run against a database system.

    A workload consists of a set of serial units. Each workload unit is identified
    by a unique `User`, and it contains a sequence (or a dynamic generator) of `Query`
    instances. Each unit must be executed serially by the runner (i.e. in a single
    thread or a single asyncio Task).

    Since workloads are immutable, they can be safely reused and composed from other
    smaller workloads.
    """

    _queries_by_users: Mapping[User, QuerySource]

    def __init__(self, queries_by_users: Mapping[User, QuerySource]) -> None:
        # Cannot use normal field assignment for frozen dataclasses
        # See https://docs.python.org/3/library/dataclasses.html#frozen-instances
        object.__setattr__(
            self,
            "_queries_by_users",
            Map(
                {
                    user: to_tuple(query_source)
                    for user, query_source in queries_by_users.items()
                }
            ),
        )

    def __iter__(self) -> Iterator[tuple[User, QuerySource]]:
        """
        Yields tuples of (User, QuerySource) with the all unlabeled users
        labeled with some nonnegative integers, for more readability.

        Note that the labeling must be deterministic across multiple iterations.
        """
        user_labels = {user.label for user in self._queries_by_users}
        int_counter = itertools.count()

        def next_unused_int() -> int:
            while (next_int := next(int_counter)) in user_labels:
                continue
            return next_int

        for user, query_source in self._queries_by_users.items():
            yield (
                # Label unlabeled users with nonnegative integers
                (user.relabel(next_unused_int()) if user.label is None else user),
                query_source,
            )

    def __len__(self) -> int:
        """Returns the number of users (i.e. serial units) in this workload."""
        return len(self._queries_by_users)

    @staticmethod
    def serial(query_source: QuerySource, *, user: User | None = None) -> Workload:
        """Creates a workload with a single serial unit."""
        if user is None:
            user = User.random()
        return Workload({user: query_source})

    @staticmethod
    def concurrent(
        query_sources: Sequence[QuerySource] | Mapping[User, QuerySource]
    ) -> Workload:
        """Creates a workload with multiple serial units."""
        if isinstance(query_sources, Sequence):
            return Workload.combine(
                Workload.serial(query_source) for query_source in query_sources
            )

        return Workload.combine(
            Workload.serial(user=user, query_source=queries)
            for user, queries in query_sources.items()
        )

    @staticmethod
    def combine(workloads: Iterable[Workload]) -> Workload:
        """
        Combine multiple workloads into a single workload.

        If more than one workloads contain the same `User` (by equality),
        the combined workload will merge all query sources by the same user
        in their original order in the given workloads.
        """
        queries_by_users: dict[User, QuerySource] = {}

        for workload in workloads:
            # pylint: disable-next=protected-access
            for user, queries in workload._queries_by_users.items():
                queries_by_users[user] = (
                    chain(queries_by_users[user], queries)
                    if user in queries_by_users
                    else queries
                )

        return Workload(queries_by_users)
