from typing import final

from attrs import define

from defio.sql.ast.expression import FunctionName
from defio.sql.schema import DataType, Schema
from defio.sqlgen.ast.expression import GenColumnReference, GenFunctionCall
from defio.sqlgen.ast.from_clause import GenFromClause
from defio.sqlgen.ast.statement import GenTargetList
from defio.utils.random import Randomizer


@final
@define(frozen=True, kw_only=True)
class AggregateSamplerConfig:
    """Configurations of an aggregate sampler."""

    max_num_aggregates: int
    p_count_star: float = 0.1
    p_count_distinct: float = 0.5

    def __attrs_post_init__(self) -> None:
        assert self.max_num_aggregates >= 1


@final
@define(frozen=True, eq=False, kw_only=True)
class AggregateSampler:
    """
    Random sampler of SQL target aggregates.

    When choosing an aggregate function, this sampler is aware of
    basic information such as the column's type and primary/foreign
    key constraints. For example, `SUM` and `AVG` is not suitable
    for string columns.

    The behavior of this sampler can be customized to some extent
    via the corresponding config.
    """

    schema: Schema
    rng: Randomizer
    config: AggregateSamplerConfig

    def sample_aggregates(self, joins: GenFromClause) -> GenTargetList:
        """
        Samples some target aggregates and returns the corresponding
        AST representation.
        """
        # Simply return a `COUNT(*)` with some probability
        if self.rng.flip(self.config.p_count_star):
            return GenTargetList(
                targets=[GenFunctionCall(func_name=FunctionName.COUNT, agg_star=True)]
            )

        possible_column_refs = [
            GenColumnReference(unique_table, column)
            for unique_table in joins.unique_tables
            for column in unique_table.columns
        ]

        # Generate at least one aggregates
        num_aggregates = self.rng.randint(
            1,
            min(len(possible_column_refs), self.config.max_num_aggregates),
            inclusive=True,
        )

        sampled_column_refs = self.rng.choose(possible_column_refs, size=num_aggregates)

        return GenTargetList(
            targets=[
                self._sample_aggregate(column_ref) for column_ref in sampled_column_refs
            ]
        )

    def _sample_aggregate(self, column_ref: GenColumnReference) -> GenFunctionCall:
        if (
            column_ref.column.dtype in (DataType.STRING, DataType.BOOLEAN)
            or column_ref.column.is_primary_key
            or column_ref.column.is_foreign_key
        ):
            allowed_aggregate_types = [FunctionName.COUNT]
        else:
            allowed_aggregate_types = list(FunctionName)

        func_name = self.rng.choose_one(allowed_aggregate_types)

        if func_name is FunctionName.COUNT:
            return GenFunctionCall(
                func_name=FunctionName.COUNT,
                agg_distinct=self.rng.flip(self.config.p_count_distinct),
                args=[column_ref],
            )

        return GenFunctionCall(func_name=func_name, args=[column_ref])
