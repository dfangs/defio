from collections.abc import Sequence
from typing import Final

import pytest

from defio.dataset.imdb import IMDB_GZ
from defio.dataset.stats import DataStats
from defio.sql.schema import Schema
from defio.sqlgen.ast.from_clause import GenFromClause
from defio.sqlgen.ast.where_clause import (
    GenCompoundPredicate,
    GenSimplePredicate,
    GenWhereClause,
)
from defio.sqlgen.sampler.join import JoinSampler, JoinSamplerConfig
from defio.sqlgen.sampler.predicate import PredicateSampler, PredicateSamplerConfig
from defio.utils.random import Randomizer

NUM_ITERS: Final = 1000


class TestPredicateSampler:
    @pytest.fixture(name="schema", scope="module")
    def fixture_schema(self) -> Schema:
        return IMDB_GZ.schema

    @pytest.fixture(name="stats", scope="module")
    def fixture_stats(self) -> DataStats:
        return IMDB_GZ.stats

    @pytest.fixture(name="sampled_joins", scope="module")
    def fixture_sampled_joins(self, schema: Schema) -> Sequence[GenFromClause]:
        join_sampler = JoinSampler(
            schema=schema,
            rng=Randomizer(),
            config=JoinSamplerConfig(
                max_num_tables=len(schema.tables),
            ),
        )

        return [join_sampler.sample_joins() for _ in range(NUM_ITERS)]

    @pytest.mark.parametrize(
        "max_num_predicates",
        [0, 1, 2, 4],
    )
    @pytest.mark.dataset
    def test_sample(
        self,
        schema: Schema,
        stats: DataStats,
        sampled_joins: Sequence[GenFromClause],
        max_num_predicates: int,
    ) -> None:
        # NOTE:
        # Use module scope for the fixtures above to avoid
        # recomputing the samples or reloading from files

        predicate_sampler = PredicateSampler(
            schema=schema,
            stats=stats,
            rng=Randomizer(),
            config=PredicateSamplerConfig(
                max_num_predicates=max_num_predicates,
            ),
        )

        sampled_predicates = [
            predicate_sampler.sample_predicates(joins) for joins in sampled_joins
        ]

        # Check whether the config is enforced
        assert all(
            TestPredicateSampler._get_num_predicates(predicates) <= max_num_predicates
            for predicates in sampled_predicates
        )

    @staticmethod
    def _get_num_predicates(predicates: GenWhereClause | None) -> int:
        match predicates:
            case None:
                return 0
            case GenSimplePredicate():
                return 1
            case GenCompoundPredicate():
                return len(predicates.children)
            case _:
                raise RuntimeError("Should not reach here")
