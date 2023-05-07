from collections.abc import Sequence, Set
from itertools import combinations, pairwise
from typing import Final

import pytest

from defio.sql.ast.from_clause import FromClause, JoinType
from defio.sql.schema import Schema, Table, TableColumn
from defio.sqlgen.ast.from_clause import GenAliasedTable, GenFromClause, GenJoin
from defio.sqlgen.sampler.join import JoinEdge, JoinSampler, JoinSamplerConfig

NUM_ITERS: Final = 3
NUM_SAMPLES: Final = 1000


class TestJoinSampler:
    @pytest.mark.parametrize(
        "max_num_joins, join_types, join_type_weights",
        [
            (0, [JoinType.INNER_JOIN], None),
            (1, [JoinType.INNER_JOIN], [1.0]),
            (2, [JoinType.INNER_JOIN, JoinType.LEFT_OUTER_JOIN], [0.8, 0.2]),
            (4, list(set(JoinType) - {JoinType.CROSS_JOIN}), None),
        ],
    )
    def test_sample_join(
        self,
        imdb_schema: Schema,
        max_num_joins: int,
        join_types: Sequence[JoinType],
        join_type_weights: Sequence[float] | None,
    ) -> None:
        join_sampler = JoinSampler(
            schema=imdb_schema,
            config=JoinSamplerConfig(
                max_num_joins=max_num_joins,
                join_types=join_types,
                join_types_weights=join_type_weights,
                with_self_join=True,
            ),
        )

        sampled_joins = [join_sampler.sample_joins() for _ in range(NUM_SAMPLES)]

        expected_join_types = (
            {
                join_type
                for join_type, weight in zip(join_types, join_type_weights)
                if weight > 0
            }
            if join_type_weights is not None
            else set(join_types)
        )

        # Check whether the config is enforced

        assert all(
            TestJoinSampler._get_num_joins(joins) <= max_num_joins
            for joins in sampled_joins
        )

        assert all(
            TestJoinSampler._get_sampled_tables(joins) <= set(imdb_schema.tables)
            for joins in sampled_joins
        )

        assert all(
            TestJoinSampler._get_sampled_join_types(joins) <= expected_join_types
            for joins in sampled_joins
        )

    def test_repeatability(
        self,
        imdb_schema: Schema,
    ) -> None:
        multiple_sampled_joins: list[list[FromClause]] = []

        for _ in range(NUM_ITERS):
            join_sampler = JoinSampler(
                schema=imdb_schema,
                config=JoinSamplerConfig(
                    max_num_joins=len(imdb_schema.tables),
                ),
                seed=0,  # Seed the sampler
            )

            # NOTE: Convert to `SQL`; `GenSQL` can't be compared due to `UniqueTable`
            multiple_sampled_joins.append(
                # Smaller number of samples is OK
                [join_sampler.sample_joins().to_sql() for _ in range(NUM_SAMPLES // 10)]
            )

        # All iterations must produce the same results
        assert all(left == right for left, right in pairwise(multiple_sampled_joins))

    @staticmethod
    def _get_num_joins(joins: GenFromClause) -> int:
        match joins:
            case GenAliasedTable():
                return 0
            case GenJoin():
                return (
                    1
                    + TestJoinSampler._get_num_joins(joins.left)
                    + TestJoinSampler._get_num_joins(joins.right)
                )
            case _:
                raise RuntimeError("Should not reach here")

    @staticmethod
    def _get_sampled_tables(joins: GenFromClause) -> Set[Table]:
        return {unique_table.table for unique_table in joins.unique_tables}

    @staticmethod
    def _get_sampled_join_types(joins: GenFromClause) -> Set[JoinType]:
        match joins:
            case GenAliasedTable():
                return set()
            case GenJoin():
                return (
                    {joins.join_type}
                    | TestJoinSampler._get_sampled_join_types(joins.left)
                    | TestJoinSampler._get_sampled_join_types(joins.right)
                )
            case _:
                raise RuntimeError("Should not reach here")


class TestJoinEdge:
    def test_equality_and_hash(self, imdb_schema: Schema) -> None:
        # Test for all pairs of table-columns, since the schema is small
        for table in imdb_schema.tables:
            for first, second in combinations(table.table_columns, 2):
                forward, reverse = JoinEdge(first, second), JoinEdge(second, first)
                assert forward == reverse
                assert hash(forward) == hash(reverse)

    def test_get_possible_join_edges(self, imdb_schema: Schema) -> None:
        # Test the whole schema
        for first_table in imdb_schema.tables:
            actual = JoinEdge.get_possible_join_edges(imdb_schema, first_table)

            expected = {
                JoinEdge(
                    TableColumn(first_table, first_column),
                    TableColumn(second_table, second_column),
                )
                for first_column in first_table.columns
                for second_table, second_column in (
                    imdb_schema.relationships.get_possible_joins(
                        first_table, first_column
                    )
                )
            }

            assert actual == expected
