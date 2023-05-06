from collections.abc import Sequence
from typing import Final

import pytest

from defio.sql.schema import Schema
from defio.sqlgen.ast.from_clause import GenFromClause
from defio.sqlgen.sampler.aggregate import AggregateSampler, AggregateSamplerConfig
from defio.sqlgen.sampler.join import JoinSampler, JoinSamplerConfig
from defio.utils.random import Randomizer

NUM_ITERS: Final = 1000


class TestAggregateSampler:
    @pytest.fixture(name="sampled_joins", scope="module")
    def fixture_sampled_joins(self, imdb_schema: Schema) -> Sequence[GenFromClause]:
        join_sampler = JoinSampler(
            schema=imdb_schema,
            rng=Randomizer(),
            config=JoinSamplerConfig(
                max_num_tables=len(imdb_schema.tables),
            ),
        )

        return [join_sampler.sample_joins() for _ in range(NUM_ITERS)]

    @pytest.mark.parametrize(
        "max_num_aggregates",
        [1, 2, 4],
    )
    def test_sample_aggregate(
        self,
        imdb_schema: Schema,
        sampled_joins: Sequence[GenFromClause],
        max_num_aggregates: int,
    ) -> None:
        aggregate_sampler = AggregateSampler(
            schema=imdb_schema,
            rng=Randomizer(),
            config=AggregateSamplerConfig(
                max_num_aggregates=max_num_aggregates,
            ),
        )

        sampled_aggregates = [
            aggregate_sampler.sample_aggregates(joins) for joins in sampled_joins
        ]

        # Check whether the config is enforced
        assert all(
            len(aggregates.targets) <= max_num_aggregates
            for aggregates in sampled_aggregates
        )
