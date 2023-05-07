from __future__ import annotations

from collections.abc import Sequence, Set
from math import isclose
from typing import final

from attrs import define, field

from defio.sql.ast.from_clause import JoinType
from defio.sql.ast.operator import BinaryOperator
from defio.sql.schema import Schema, Table, TableColumn
from defio.sqlgen.ast.expression import GenColumnReference
from defio.sqlgen.ast.from_clause import (
    GenAliasedTable,
    GenFromClause,
    GenJoin,
    JoinPredicate,
)
from defio.sqlgen.ast.helper import UniqueTable
from defio.sqlgen.utils import sort_join_edges
from defio.utils.random import Randomizer


@final
@define(frozen=True, kw_only=True)
class JoinSamplerConfig:
    """
    Configurations of a join sampler.

    NOTE:
    It is possible that `num_joins` >= `num_tables` (e.g., self-joins).
    Thus, it is more appropriate to parameterize using `max_num_joins`
    rather than `max_num_tables`.
    """

    max_num_joins: int
    join_types: Sequence[JoinType] = (JoinType.INNER_JOIN,)
    join_types_weights: Sequence[float] | None = None
    with_self_join: bool = False

    def __attrs_post_init__(self) -> None:
        assert self.max_num_joins >= 0
        assert JoinType.CROSS_JOIN not in self.join_types
        assert self.join_types_weights is None or (
            len(self.join_types_weights) == len(self.join_types)
        )
        assert self.join_types_weights is None or (
            isclose(sum(self.join_types_weights), 1)
        )


@final
@define(frozen=True, eq=False, kw_only=True)
class JoinSampler:
    """
    Random sampler of SQL table joins based on their relationship graph.

    This sampler does not require the relationship graph to be acyclic
    (i.e. it can handle self-joins). Additionally, it only generates
    equijoins based on foreign-key relationships.

    The behavior of this sampler can be customized to some extent
    via the corresponding config.
    """

    schema: Schema
    config: JoinSamplerConfig
    seed: int | None = None
    _rng: Randomizer = field(init=False)

    def __attrs_post_init__(self) -> None:
        object.__setattr__(self, "_rng", Randomizer(self.seed))

    def sample_joins(self) -> GenFromClause:
        """
        Samples some table joins and returns the corresponding
        AST representation.
        """
        # Start with a random table
        initial_table = self._rng.choose_one(self.schema.tables)
        initial_unique_table = UniqueTable(initial_table)

        join_tables = {initial_table: initial_unique_table}
        possible_join_edges = {
            *JoinEdge.get_possible_join_edges(self.schema, initial_table)
        }
        joins: GenFromClause = GenAliasedTable(initial_unique_table)

        # Choose a random number of joins
        num_joins = self._rng.randint(self.config.max_num_joins, inclusive=True)

        for _ in range(num_joins):
            # Terminate early if there are no more possible joins
            if len(possible_join_edges) == 0:
                break

            # Convert Set to a sorted Sequence
            # NOTE: Set iteration order is not deterministic, so this is necessary
            join_edge = self._rng.choose_one(sort_join_edges(possible_join_edges))

            first_table, first_column = join_edge.first
            second_table, second_column = join_edge.second

            if first_table in join_tables and second_table in join_tables:
                left_unique_table = join_tables[first_table]

                if first_table == second_table:
                    # Special case: Self-join
                    if not self.config.with_self_join:
                        continue
                    right_unique_table = UniqueTable(second_table)
                else:
                    right_unique_table = join_tables[second_table]

                left_column, right_column = first_column, second_column

            elif first_table in join_tables and second_table not in join_tables:
                left_unique_table = join_tables[first_table]
                right_unique_table = UniqueTable(second_table)
                left_column, right_column = first_column, second_column

                join_tables[second_table] = right_unique_table
                possible_join_edges |= JoinEdge.get_possible_join_edges(
                    self.schema, second_table
                )

            elif first_table not in join_tables and second_table in join_tables:
                # Make sure the left table/column is always the one already joined
                left_unique_table = join_tables[second_table]
                right_unique_table = UniqueTable(first_table)
                left_column, right_column = second_column, first_column

                join_tables[first_table] = right_unique_table
                possible_join_edges |= JoinEdge.get_possible_join_edges(
                    self.schema, first_table
                )

            else:
                raise RuntimeError(
                    "At least one of the tables must have already been joined"
                )

            # Remove the selected edge from the pool
            possible_join_edges -= {join_edge}

            # Randomly select the join type
            join_type = self._rng.choose_one(
                self.config.join_types, weights=self.config.join_types_weights
            )

            joins = GenJoin(
                left=joins,
                join_type=join_type,
                right=GenAliasedTable(right_unique_table),
                predicate=JoinPredicate(
                    left=GenColumnReference(left_unique_table, left_column),
                    operator=BinaryOperator.EQ,  # Always equijoin
                    right=GenColumnReference(right_unique_table, right_column),
                ),
            )

        return joins


@final
@define(frozen=True)
class JoinEdge:
    """
    Represents a join edge, i.e. two table-column pairs that can be joined.

    A join edge is undirectional: `A.a JOIN B.b` is the same as `B.b JOIN A.a`.
    """

    first: TableColumn
    second: TableColumn

    def __eq__(self, other) -> bool:
        return isinstance(other, JoinEdge) and (
            frozenset([self.first, self.second])
            == frozenset([other.first, other.second])
        )

    def __hash__(self) -> int:
        return hash(frozenset([self.first, self.second]))

    @staticmethod
    def get_possible_join_edges(schema: Schema, table: Table) -> Set[JoinEdge]:
        """
        Returns the set of all possible join edges that involve the given table.
        """
        first_table = table  # Rename for readability

        return {
            JoinEdge(
                TableColumn(first_table, first_column),
                TableColumn(second_table, second_column),
            )
            for first_column in first_table.columns
            for second_table, second_column in schema.relationships.get_possible_joins(
                first_table, first_column
            )
        }
