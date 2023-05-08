from collections.abc import Set
from itertools import pairwise
from typing import Final

import pytest

from defio.dataset.imdb import IMDB_GZ
from defio.sql.ast.from_clause import AliasedTable, FromClause, Join, JoinType
from defio.sql.ast.statement import SelectStatement
from defio.sql.ast.where_clause import CompoundPredicate, SimplePredicate, WhereClause
from defio.sql.parser import parse_sql
from defio.sqlgen.generator import RandomSqlGenerator
from defio.sqlgen.sampler.aggregate import AggregateSamplerConfig
from defio.sqlgen.sampler.join import JoinSamplerConfig
from defio.sqlgen.sampler.predicate import PredicateSamplerConfig

NUM_ITERS: Final = 3
NUM_SAMPLES: Final = 1000


class TestRandomSqlGenerator:
    @pytest.mark.dataset
    def test_generate_sql(self) -> None:
        generator = RandomSqlGenerator(
            dataset=IMDB_GZ,
            join_config=JoinSamplerConfig(
                # Note: Number of joins can be greater than or equal to number of tables
                max_num_joins=(max_num_joins := len(IMDB_GZ.schema.tables)),
                join_types=list(join_types := set(JoinType) - {JoinType.CROSS_JOIN}),
                acyclic=False,
            ),
            predicate_config=PredicateSamplerConfig(
                max_num_predicates=(max_num_predicates := 10),
            ),
            aggregate_config=AggregateSamplerConfig(
                max_num_aggregates=(max_num_aggregates := 3),
            ),
            num_queries=NUM_SAMPLES,
        )

        for sql in generator:
            # All generated SQLs must be parsable
            parsed_sqls = parse_sql(sql)
            assert len(parsed_sqls) == 1

            # For now, all generated SQLs are `SELECT` statements
            parsed_sql = parsed_sqls[0]
            assert isinstance(parsed_sql, SelectStatement)

            # Check whether the config is enforced
            assert parsed_sql.from_clause is not None
            assert _get_num_joins(parsed_sql.from_clause) <= max_num_joins
            assert _get_join_types(parsed_sql.from_clause) <= join_types

            if parsed_sql.where_clause is not None:
                assert (
                    _get_num_predicates(parsed_sql.where_clause) <= max_num_predicates
                )

            assert len(parsed_sql.target_list.targets) <= max_num_aggregates

    @pytest.mark.dataset
    def test_repeatability(self) -> None:
        # NOTE: Even without seeding, a generator must produce repeatable outputs
        generator = RandomSqlGenerator(
            dataset=IMDB_GZ,
            join_config=JoinSamplerConfig(
                max_num_joins=len(IMDB_GZ.schema.tables),
                join_types=list(set(JoinType) - {JoinType.CROSS_JOIN}),
            ),
            predicate_config=PredicateSamplerConfig(
                max_num_predicates=10,
            ),
            aggregate_config=AggregateSamplerConfig(
                max_num_aggregates=3,
            ),
            num_queries=NUM_SAMPLES // 10,  # Smaller number of samples is OK
        )

        multiple_generated_sql_queries = [
            [parse_sql(sql) for sql in generator] for _ in range(NUM_ITERS)
        ]

        assert all(
            left == right for left, right in pairwise(multiple_generated_sql_queries)
        )


def _get_num_joins(from_clause: FromClause) -> int:
    match from_clause:
        case AliasedTable():
            return 0
        case Join() as join:
            return 1 + _get_num_joins(join.left) + _get_num_joins(join.right)
        case _:
            raise RuntimeError("Should not reach here")


def _get_join_types(from_clause: FromClause) -> Set[JoinType]:
    match from_clause:
        case AliasedTable():
            return set()
        case Join() as join:
            return (
                {join.join_type}
                | _get_join_types(join.left)
                | _get_join_types(join.right)
            )
        case _:
            raise RuntimeError("Should not reach here")


def _get_num_predicates(where_clause: WhereClause) -> int:
    match where_clause:
        case SimplePredicate():
            return 1
        case CompoundPredicate() as compound:
            return sum(_get_num_predicates(child) for child in compound.children)
        case _:
            raise RuntimeError("Should not reach here")
