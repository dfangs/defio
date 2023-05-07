import math
from typing import Any, assert_never, cast

import numpy as np
from attrs import define, field

from defio.dataset.column_stats import (
    CategoricalColumnStats,
    KeyColumnStats,
    NumericalColumnStats,
)
from defio.dataset.stats import (
    BooleanColumnStats,
    DataStats,
    FloatColumnStats,
    IntegerColumnStats,
    RawStringColumnStats,
    StringColumnStats,
)
from defio.sql.ast.operator import BinaryOperator
from defio.sql.schema import DataType, Schema
from defio.sqlgen.ast.expression import GenColumnReference
from defio.sqlgen.ast.from_clause import GenFromClause
from defio.sqlgen.ast.where_clause import (
    GenCompoundPredicate,
    GenSimplePredicate,
    GenWhereClause,
)
from defio.sqlgen.utils import sort_unique_tables
from defio.utils.random import Randomizer


@define(frozen=True, kw_only=True)
class PredicateSamplerConfig:
    """Configurations of a predicate sampler."""

    max_num_predicates: int
    p_drop_point_query: float = 0.9
    p_not: float = 0.05

    def __attrs_post_init__(self) -> None:
        assert self.max_num_predicates >= 0
        assert 0 <= self.p_drop_point_query <= 1
        assert 0 <= self.p_not <= 1


@define(frozen=True, eq=False, kw_only=True)
class PredicateSampler:
    """
    Random sampler of SQL filter predicates based on column statistics.

    This sampler works by sampling some columns from the given table joins
    and generating the suitable predicates based on the corresponding
    column statistics (which are based on the data type and column type).

    The behavior of this sampler can be customized to some extent
    via the corresponding config.
    """

    schema: Schema
    stats: DataStats
    config: PredicateSamplerConfig
    seed: int | None = None
    _rng: Randomizer = field(init=False)

    def __attrs_post_init__(self) -> None:
        object.__setattr__(self, "_rng", Randomizer(self.seed))

    def sample_predicates(self, joins: GenFromClause) -> GenWhereClause | None:
        """
        Samples some filter predicates and returns the corresponding
        AST representation (if any).
        """
        # Convert Set to sorted Sequence (to match with `weights`)
        unique_tables = sort_unique_tables(joins.unique_tables)

        possible_column_refs = [
            GenColumnReference(unique_table, column)
            for unique_table in unique_tables
            for column in unique_table.columns
        ]

        # Weight the probability of being sampled by # of columns in the table
        # so that we don't have too many predicates on the same table
        weights = np.array(
            [
                1 / len(unique_table.columns)
                for unique_table in unique_tables
                for _ in unique_table.columns
            ]
        )
        weights /= np.sum(weights)

        num_predicates = self._rng.randint(
            min(len(possible_column_refs), self.config.max_num_predicates),
            inclusive=True,
        )

        # `WHERE` clause is allowed to be empty (i.e. no predicates)
        if num_predicates == 0:
            return None

        sampled_column_refs = self._rng.choose(
            possible_column_refs, size=num_predicates, weights=weights
        )

        # Some of the initially chosen columns may not generate a predicate
        # In this case, simply take what's left (i.e. no need to resample new columns)
        sampled_predicates = [
            (
                # Invert the predicate with some probability
                GenCompoundPredicate.make_not(predicate)
                if self._rng.flip(self.config.p_not)
                else predicate
            )
            for column_ref in sampled_column_refs
            if (predicate := self._sample_predicate(column_ref)) is not None
        ]

        if len(sampled_predicates) == 0:
            return None

        # Compound predicate `AND` requires >1 children
        if len(sampled_predicates) == 1:
            return sampled_predicates[0]

        return GenCompoundPredicate.make_and(sampled_predicates)

    def _sample_predicate(
        self, column_ref: GenColumnReference
    ) -> GenSimplePredicate | None:
        """
        Samples a predicate from the given column reference based on
        the data type and the column type.

        Returns `None` if the predicate can't be sampled for some reason
        (e.g., insufficient statistics).
        """
        unique_table, column = column_ref.unique_table, column_ref.column
        column_stats = self.stats.get(unique_table.table).get(column)

        # NOTE: Need to cast manually here since `isinstance` cannot be used on generics

        match dtype := column.dtype:
            case DataType.INTEGER:
                column_stats = cast(IntegerColumnStats, column_stats)
                return self._sample_integer_predicate(column_ref, column_stats)
            case DataType.FLOAT:
                column_stats = cast(FloatColumnStats, column_stats)
                return self._sample_float_predicate(column_ref, column_stats)
            case DataType.STRING:
                column_stats = cast(StringColumnStats, column_stats)
                return self._sample_string_predicate(column_ref, column_stats)
            case DataType.BOOLEAN:
                column_stats = cast(BooleanColumnStats, column_stats)
                return self._sample_boolean_predicate(column_ref, column_stats)
            case _:
                assert_never(dtype)

    def _sample_integer_predicate(
        self, column_ref: GenColumnReference, column_stats: IntegerColumnStats
    ) -> GenSimplePredicate | None:
        match column_stats:
            case CategoricalColumnStats():
                return self._sample_categorical_predicate(column_ref, column_stats)
            case KeyColumnStats():
                return self._sample_key_predicate(column_ref, column_stats)
            case NumericalColumnStats():
                return self._sample_numerical_predicate(column_ref, column_stats)
            case _:
                assert_never(column_stats)

    def _sample_float_predicate(
        self, column_ref: GenColumnReference, column_stats: FloatColumnStats
    ) -> GenSimplePredicate | None:
        match column_stats:
            case NumericalColumnStats():
                return self._sample_numerical_predicate(column_ref, column_stats)
            case _:
                assert_never(column_stats)

    def _sample_string_predicate(
        self, column_ref: GenColumnReference, column_stats: StringColumnStats
    ) -> GenSimplePredicate | None:
        match column_stats:
            case CategoricalColumnStats():
                return self._sample_categorical_predicate(column_ref, column_stats)
            case KeyColumnStats():
                return self._sample_key_predicate(column_ref, column_stats)
            case RawStringColumnStats():
                return self._sample_raw_string_predicate(column_ref, column_stats)
            case _:
                assert_never(column_stats)

    def _sample_boolean_predicate(
        self, column_ref: GenColumnReference, column_stats: BooleanColumnStats
    ) -> GenSimplePredicate | None:
        match column_stats:
            case CategoricalColumnStats():
                return self._sample_categorical_predicate(column_ref, column_stats)
            case _:
                assert_never(column_stats)

    def _sample_categorical_predicate(
        self,
        column_ref: GenColumnReference,
        column_stats: CategoricalColumnStats[Any],
    ) -> GenSimplePredicate | None:
        """
        Samples a predicate for a column with `CATEGORICAL` column type.

        Available operators: `=`, `!=`, and `IN`.
        """
        most_frequent_values = sorted(list(column_stats.most_frequent_values))

        # Edge case: No most frequent values
        if len(most_frequent_values) == 0:
            return None

        operator = self._rng.choose_one(
            [BinaryOperator.EQ, BinaryOperator.NEQ, BinaryOperator.IN]
        )

        if operator is BinaryOperator.IN:
            return GenSimplePredicate.make_binary_column_predicate(
                left=column_ref,
                operator=BinaryOperator.IN,
                right=self._rng.choose(
                    most_frequent_values,
                    size=self._rng.randint(
                        1, len(most_frequent_values), inclusive=True
                    ),
                ),
            )

        return GenSimplePredicate.make_binary_column_predicate(
            left=column_ref,
            operator=operator,
            right=self._rng.choose_one(most_frequent_values),
        )

    def _sample_key_predicate(
        self, column_ref: GenColumnReference, stats: KeyColumnStats[Any]
    ) -> GenSimplePredicate | None:
        """
        Samples a predicate for a column with `KEY` column type.

        Available operators: `=` (point query).
        """
        # Edge case: No sampled values
        if len(stats.sampled_values) == 0:
            return None

        # Randomly drop point queries so that generated predicates are not too selective
        if self._rng.flip(self.config.p_drop_point_query):
            return None

        return GenSimplePredicate.make_binary_column_predicate(
            left=column_ref,
            operator=BinaryOperator.EQ,
            right=self._rng.choose_one(stats.sampled_values),
        )

    def _sample_numerical_predicate(
        self, column_ref: GenColumnReference, stats: NumericalColumnStats
    ) -> GenSimplePredicate | None:
        """
        Samples a predicate for a column with `KEY` column type.

        Available operators: `<`, `<=`, `>`, `>=`, and `BETWEEN` (range query).
        """
        # Edge case: All values are null, in which case the stats become useless
        if math.isnan(stats.mean):
            return None

        operator = self._rng.choose_one(
            [
                BinaryOperator.LT,
                BinaryOperator.LEQ,
                BinaryOperator.LT,
                BinaryOperator.LEQ,
                BinaryOperator.BETWEEN,
                BinaryOperator.NOT_BETWEEN,
            ]
        )

        if operator in (BinaryOperator.BETWEEN, BinaryOperator.NOT_BETWEEN):
            return GenSimplePredicate.make_binary_column_predicate(
                left=column_ref,
                operator=operator,
                right=self._rng.choose(stats.percentiles, size=2),
            )

        return GenSimplePredicate.make_binary_column_predicate(
            left=column_ref,
            operator=operator,
            right=self._rng.choose_one(stats.percentiles),
        )

    def _sample_raw_string_predicate(
        self, column_ref: GenColumnReference, stats: RawStringColumnStats
    ) -> GenSimplePredicate | None:
        """
        Samples a predicate for a column with `RAW_STRING` column type.

        Available operators: `LIKE`.
        """
        frequent_words = sorted(list(stats.frequent_words))

        # Edge case: No frequent words
        if len(frequent_words) == 0:
            return None

        return GenSimplePredicate.make_binary_column_predicate(
            left=column_ref,
            operator=BinaryOperator.LIKE,
            right=f"%{self._rng.choose_one(frequent_words)}%",
        )
