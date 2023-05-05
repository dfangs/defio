from collections.abc import Sequence, Set
from datetime import datetime
from typing import Final

import pytest

from defio.workload import Workload
from defio.workload.query import QueryGenerator, QuerySource
from defio.workload.schedule import Once
from defio.workload.user import User


def make_query_generator(num_items: int) -> QueryGenerator:
    return QueryGenerator.with_fixed_time(
        list(f"SELECT {i};" for i in range(num_items)),
        schedule=Once(at=datetime(year=2022, month=10, day=1)),
    )


@pytest.fixture(name="query_generator")
def fixture_query_generator() -> QueryGenerator:
    return make_query_generator(10)


@pytest.fixture(name="query_sources")
def fixture_query_sources() -> Sequence[QuerySource]:
    small_query_generator = make_query_generator(3)
    large_query_generator = make_query_generator(4)
    query_list = list(small_query_generator)

    # Intentionally create aliases here
    return [small_query_generator, query_list, large_query_generator, query_list]


NUM_ITERS: Final = 3


class TestSerialWorkload:
    def test_without_user(self, query_generator: QueryGenerator) -> None:
        serial = Workload.serial(query_generator)
        assert len(serial) == 1

        # Workload can be iterated multiple times
        for _ in range(NUM_ITERS):
            serial_list = list(serial)
            assert len(serial_list) == 1

            _, query_source = serial_list[0]
            assert list(query_source) == list(query_generator)

    def test_with_user(self, query_generator: QueryGenerator) -> None:
        serial = Workload.serial(
            list(query_generator), user=(expected_user := User.random())
        )
        assert len(serial) == 1

        # Workload can be iterated multiple times
        for _ in range(NUM_ITERS):
            serial_list = list(serial)
            assert len(serial_list) == 1

            user, query_source = serial_list[0]
            assert user == expected_user
            assert list(query_source) == list(query_generator)

    def test_iter_relabeling(self, query_generator: QueryGenerator) -> None:
        unlabeled = Workload.serial(query_generator)
        labeled = Workload.serial(query_generator, user=User.with_label(label := "tim"))

        relabeled_user, _ = next(iter(unlabeled))
        assert isinstance(relabeled_user.label, int)
        assert relabeled_user.label >= 0

        same_labeled_user, _ = next(iter(labeled))
        assert isinstance(same_labeled_user.label, str)
        assert same_labeled_user.label == label


class TestConcurrentWorkload:
    def test_without_users(self, query_sources: Sequence[QuerySource]) -> None:
        concurrent = Workload.concurrent(query_sources)
        assert len(concurrent) == len(query_sources)

        # Workload can be iterated multiple times
        for _ in range(NUM_ITERS):
            concurrent_list = list(concurrent)

            actual_queries = {
                tuple(query_source) for _, query_source in concurrent_list
            }
            expected_queries = {tuple(query_source) for query_source in query_sources}

            assert actual_queries == expected_queries

    def test_with_users(self, query_sources: Sequence[QuerySource]) -> None:
        query_map = {User.random(): query_source for query_source in query_sources}
        concurrent = Workload.concurrent(query_map)
        assert len(concurrent) == len(query_sources)

        # Workload can be iterated multiple times
        for _ in range(NUM_ITERS):
            concurrent_list = list(concurrent)

            actual_queries = {
                user: list(query_source) for user, query_source in concurrent_list
            }
            expected_queries = {
                user: list(query_source) for user, query_source in query_map.items()
            }

            assert actual_queries == expected_queries

    def test_iter_relabeling_deterministic(
        self, query_sources: Sequence[QuerySource]
    ) -> None:
        def get_user_labels(workload: Workload) -> Set[str | int | None]:
            return {user.label for user, _ in workload}

        concurrent = Workload.concurrent(query_sources)
        first_labels = get_user_labels(concurrent)

        # Subsequent iterations of a workload must produce deterministic user labels
        for _ in range(NUM_ITERS):
            assert get_user_labels(concurrent) == first_labels


class TestCombineWorkload:
    def test_without_user_overlap(self, query_sources: Sequence[QuerySource]) -> None:
        query_map = {User.random(): query_source for query_source in query_sources}
        combined = Workload.combine(
            Workload.serial(query_source, user=user)
            for user, query_source in query_map.items()
        )
        assert len(combined) == len(query_sources)

        # Workload can be iterated multiple times
        for _ in range(NUM_ITERS):
            combined_list = list(combined)

            actual_queries = {
                user: list(query_source) for user, query_source in combined_list
            }
            expected_queries = {
                user: list(query_source) for user, query_source in query_map.items()
            }

            assert actual_queries == expected_queries

    def test_with_user_overlap(
        self, query_sources: Sequence[QuerySource], query_generator: QueryGenerator
    ) -> None:
        query_map = {User.random(): query_source for query_source in query_sources}
        dupe_user = next(iter(query_map.keys()))

        combined = Workload.combine(
            [
                Workload.concurrent(query_map),
                Workload.serial(query_generator, user=dupe_user),
            ]
        )
        assert len(combined) == len(query_sources)

        # Workload can be iterated multiple times
        for _ in range(NUM_ITERS):
            combined_list = list(combined)
            assert len(combined_list) == len(query_map)

            for user, query_source in combined_list:
                assert user in query_map

                expected_queries = (
                    list(query_map[user]) + list(query_generator)
                    if user == dupe_user
                    else list(query_map[user])
                )

                assert list(query_source) == expected_queries
