import bisect
from collections.abc import Mapping, Sequence, Set
from itertools import chain
from math import isclose, log10

import numpy as np
from attrs import define
from numpy.typing import NDArray

from defio.dataset.column_stats import (
    CategoricalColumnStats,
    KeyColumnStats,
    NumericalColumnStats,
    RawStringColumnStats,
)
from defio.dataset.stats import DataStats
from defio.sql.ast.expression import (
    BinaryExpression,
    ColumnReference,
    Constant,
    Expression,
    UnaryExpression,
)
from defio.sql.ast.from_clause import AliasedTable, FromClause, Join
from defio.sql.ast.operator import BinaryOperator, LogicalOperator
from defio.sql.ast.statement import SelectStatement
from defio.sql.ast.where_clause import CompoundPredicate, SimplePredicate, WhereClause
from defio.sql.parser import parse_sql
from defio.sql.schema import Column, DataType, Schema, Table


@define
class Featurizer:
    _schema: Schema
    _stats: DataStats
    _table_sizes: Mapping[str, float]

    def featurize(self, sql: str) -> NDArray[np.float64]:
        statements = parse_sql(sql)

        assert len(statements) == 1
        statement = statements[0]

        assert isinstance(statement, SelectStatement)

        num_features = 4
        features = np.zeros(num_features * len(self._schema.tables))

        for table in self._get_join_tables(statement):
            idx = self._schema.tables.index(table)
            features[num_features * idx] = 1

        for column in self._get_predicate_columns(statement):
            idx = self._schema.tables.index(self._find_table(column))
            features[num_features * idx + 1] = int(column.is_primary_key)
            features[num_features * idx + 2] = int(column.is_foreign_key)

        for table, selectivity in self._get_table_selectivities(statement).items():
            idx = self._schema.tables.index(table)
            cardinality = selectivity * self._table_sizes[table.name]
            features[num_features * idx + 3] = (
                0 if isclose(cardinality, 0) else log10(cardinality)
            )

        return features

        # predicate_columns = self._get_predicate_columns(statement)
        # column_features = []
        # for table in self._schema.tables:
        #     for column in table.columns:
        #         column_features.append(int(column in predicate_columns))
        # return np.concatenate((features, np.array(column_features)))

    def _find_table(self, column: Column) -> Table:
        # TODO: Rethink
        for table in self._schema.tables:
            if column in table.columns:
                return table
        raise ValueError(f"Column {column.name} not in schema")

    def _resolve_table(
        self, table_name_or_alias: str, from_clause: FromClause
    ) -> Table:
        table_aliases = {
            (
                aliased_table.alias
                if aliased_table.alias is not None
                else aliased_table.name
            ): self._schema.get_table(aliased_table.name)
            for aliased_table in self._get_aliased_tables(from_clause)
        }

        return table_aliases[table_name_or_alias]

    def _resolve_column(
        self,
        column_name: str,
        table_name_or_alias: str | None,
        from_clause: FromClause,
    ) -> Column:
        if table_name_or_alias is not None:
            table = self._resolve_table(table_name_or_alias, from_clause)
            return table.get_column(column_name)

        names_to_columns: dict[str, list[Column]] = {}
        for table in self._schema.tables:
            for column in table.columns:
                names_to_columns.setdefault(column.name, []).append(column)

        columns = names_to_columns[column_name]
        if len(columns) != 1:
            raise ValueError("Ambiguous column")
        return columns[0]

    def _get_aliased_tables(self, from_clause: FromClause) -> Set[AliasedTable]:
        match from_clause:
            case AliasedTable() as aliased_table:
                return {aliased_table}
            case Join() as join:
                return self._get_aliased_tables(join.left) | self._get_aliased_tables(
                    join.right
                )
            case _:
                raise RuntimeError("Should not reach here")

    def _get_join_tables(self, statement: SelectStatement) -> Set[Table]:
        assert statement.from_clause is not None  # TODO: Clean up
        return {
            self._resolve_table(aliased_table.name, statement.from_clause)
            for aliased_table in self._get_aliased_tables(statement.from_clause)
        }

    def _get_predicate_columns(self, statement: SelectStatement) -> Set[Column]:
        def recurse_where(where_clause: WhereClause) -> Set[Column]:
            match where_clause:
                case SimplePredicate():
                    return recurse_expr(where_clause.expression)

                case CompoundPredicate():
                    return set(
                        chain.from_iterable(
                            recurse_where(child) for child in where_clause.children
                        )
                    )

                case _:
                    raise RuntimeError("Should not reach here")

        def recurse_expr(expr: Expression) -> Set[Column]:
            assert statement.from_clause is not None  # TODO: Clean up

            match expr:
                case UnaryExpression():
                    return recurse_expr(expr.operand)

                case BinaryExpression():
                    return recurse_expr(expr.left) | (
                        set(
                            chain.from_iterable(
                                recurse_expr(child) for child in expr.right
                            )
                        )
                        if isinstance(expr.right, Sequence)
                        else recurse_expr(expr.right)
                    )

                case ColumnReference():
                    return {
                        self._resolve_column(
                            expr.column_name, expr.table_alias, statement.from_clause
                        )
                    }

                case _:
                    return set()

        if statement.where_clause is None:
            return set()
        return recurse_where(statement.where_clause)

    def _get_table_selectivities(
        self, statement: SelectStatement
    ) -> Mapping[Table, float]:
        def recurse_where(where_clause: WhereClause) -> Mapping[Table, float]:
            match where_clause:
                case SimplePredicate():
                    return recurse_expr(where_clause.expression)

                case CompoundPredicate():
                    all_selectivities = [
                        recurse_where(child) for child in where_clause.children
                    ]

                    match where_clause.operator:
                        case LogicalOperator.AND:
                            combined: dict[Table, float] = {}
                            for selectivities in all_selectivities:
                                for table, selectivity in selectivities.items():
                                    if selectivity < 0:
                                        print(statement)
                                    combined[table] = (
                                        combined.get(table, 1) * selectivity
                                    )
                            return combined

                        case LogicalOperator.OR:
                            raise NotImplementedError

                        case LogicalOperator.NOT:
                            assert len(all_selectivities) == 1
                            return {
                                table: 1 - selectivity
                                for table, selectivity in all_selectivities[0].items()
                            }

                case _:
                    raise RuntimeError("Should not reach here")

        def recurse_expr(expr: Expression) -> Mapping[Table, float]:
            assert statement.from_clause is not None  # TODO: Clean up

            match expr:
                case BinaryExpression():
                    # TODO: Assume this format for now
                    assert isinstance(expr.left, ColumnReference)

                    column = self._resolve_column(
                        expr.left.column_name,
                        expr.left.table_alias,
                        statement.from_clause,
                    )
                    table = self._find_table(column)

                    # NOTE: This ensures any rounding errors won't bubble up
                    selectivity = max(
                        0,
                        min(
                            1,
                            self._get_selectivity(
                                table, column, expr.operator, expr.right
                            ),
                        ),
                    )

                    if selectivity > 1:
                        print(selectivity, expr)

                    return {table: selectivity}

                case _:
                    # TODO: Only consider binary expressions
                    return {}

        if statement.where_clause is None:
            return {}
        return recurse_where(statement.where_clause)

    def _get_selectivity(
        self,
        table: Table,
        column: Column,
        operator: BinaryOperator,
        right: Expression | Sequence[Expression],
    ) -> float:
        stats = self._stats.get(table).get(column)
        match stats:
            case CategoricalColumnStats():
                values = []
                if operator is BinaryOperator.IN:
                    assert isinstance(right, Sequence)
                    for child in right:
                        assert isinstance(child, Constant)
                        values.append(child.value)
                else:
                    assert isinstance(right, Constant)
                    values.append(right.value)

                freq = 0
                for value in values:
                    if column.dtype is DataType.INTEGER:
                        value = int(value)
                    elif column.dtype is DataType.BOOLEAN:
                        value = bool(value)

                    if value in stats.most_frequent_values:
                        freq += stats.most_frequent_values[value]

                if operator is BinaryOperator.NEQ:
                    return 1 - freq
                return freq

            case NumericalColumnStats():
                if operator in (BinaryOperator.BETWEEN, BinaryOperator.NOT_BETWEEN):
                    assert isinstance(right, Sequence)
                    low, high = right[0], right[1]

                    assert isinstance(low, Constant)
                    assert isinstance(high, Constant)

                    # TODO: There is a bug in `BETWEEN` implementation
                    percent = max(
                        0,
                        bisect.bisect_left(stats.percentiles, float(high.value))
                        - bisect.bisect_left(stats.percentiles, float(low.value)),
                    )

                    if operator is BinaryOperator.NOT_BETWEEN:
                        return 1 - percent / 100
                    return percent / 100

                assert isinstance(right, Constant)
                percent = bisect.bisect_left(stats.percentiles, float(right.value))

                if operator in (BinaryOperator.LT, BinaryOperator.LEQ):
                    return percent / 100
                return 1 - percent / 100

            case KeyColumnStats():
                return 1 / stats.num_unique

            case RawStringColumnStats():
                assert isinstance(right, Constant)
                assert isinstance(right.value, str)
                return stats.frequent_words[right.value.replace("%", "")]

            case _:
                raise RuntimeError("Should not reach here")
