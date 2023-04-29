from __future__ import annotations

import itertools
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import final

from attrs import define, field
from immutables import Map

from defio.utils.attrs import to_map, to_tuple
from defio.workload.query import ChainedQueries, Queries
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

    _queries_by_users: Mapping[User, Queries] = field(
        converter=to_map, default=Map, alias="_queries_by_users"
    )

    def __iter__(self) -> Iterator[tuple[User, Queries]]:
        user_labels = {user.label for user in self._queries_by_users}
        int_counter = itertools.count()

        def next_unused_int() -> int:
            while (next_int := next(int_counter)) in user_labels:
                continue
            return next_int

        for user, queries in self._queries_by_users.items():
            yield (
                # Label unlabeled users with nonnegative integers
                (user.relabel(next_unused_int()) if user.label is None else user),
                queries,
            )

    @staticmethod
    def serial(queries: Queries, *, user: User | None = None) -> Workload:
        """Creates a `Workload` with a single serial unit."""
        if user is None:
            user = User.random()
        return Workload({user: to_tuple(queries)})  # Defensive copy

    @staticmethod
    def concurrent(all_queries: Sequence[Queries] | Mapping[User, Queries]) -> Workload:
        """Creates a `Workload` with multiple serial units."""
        if isinstance(all_queries, Sequence):
            return Workload.combine(Workload.serial(queries) for queries in all_queries)

        return Workload.combine(
            Workload.serial(user=user, queries=queries)
            for user, queries in all_queries.items()
        )

    @staticmethod
    def combine(workloads: Iterable[Workload]) -> Workload:
        """
        Combine multiple workloads into a single `Workload`.

        If more than one workloads define the same `User` (by equality), the combined
        workload will merge the queries in the original order of the workloads.
        """
        queries_by_users = dict[User, Queries]()

        for workload in workloads:
            # pylint: disable-next=protected-access
            for user, queries in workload._queries_by_users.items():
                # No need for defensive copy here
                queries_by_users[user] = (
                    ChainedQueries(queries_by_users[user], queries)
                    if user in queries_by_users
                    else queries
                )

        return Workload(queries_by_users)

    @staticmethod
    def from_ziniu() -> Workload:
        raise NotImplementedError
