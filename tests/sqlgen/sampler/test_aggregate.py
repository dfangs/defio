from collections.abc import Sequence
from itertools import pairwise
from typing import Final

import pytest

from defio.sql.ast.statement import TargetList
from defio.sql.schema import Schema
from defio.sqlgen.ast.from_clause import GenFromClause
from defio.sqlgen.sampler.aggregate import AggregateSampler, AggregateSamplerConfig
from defio.sqlgen.sampler.join import JoinSampler, JoinSamplerConfig

NUM_ITERS: Final = 3
NUM_SAMPLES: Final = 1000


class TestAggregateSampler:
    @pytest.fixture(name="sampled_joins", scope="module")
    def fixture_sampled_joins(self, imdb_schema: Schema) -> Sequence[GenFromClause]:
        join_sampler = JoinSampler(
            schema=imdb_schema,
            config=JoinSamplerConfig(
                max_num_joins=len(imdb_schema.tables),
            ),
        )

        return [join_sampler.sample_joins() for _ in range(NUM_SAMPLES)]

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

    def test_repeatability(
        self,
        imdb_schema: Schema,
        sampled_joins: Sequence[GenFromClause],
    ) -> None:
        multiple_sampled_aggregates: list[list[TargetList]] = []

        for _ in range(NUM_ITERS):
            aggregate_sampler = AggregateSampler(
                schema=imdb_schema,
                config=AggregateSamplerConfig(
                    max_num_aggregates=3,
                ),
                seed=0,  # Seed the sampler
            )

            # NOTE: Convert to `SQL`; `GenSQL` can't be compared due to `UniqueTable`
            multiple_sampled_aggregates.append(
                [
                    aggregate_sampler.sample_aggregates(joins).to_sql()
                    for i, joins in enumerate(sampled_joins)
                    if i < NUM_SAMPLES // 10  # Smaller number of samples is OK
                ]
            )

        # All iterations must produce the same results
        assert all(
            left == right for left, right in pairwise(multiple_sampled_aggregates)
        )
