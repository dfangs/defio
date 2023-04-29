from collections.abc import Mapping, Sequence
from typing import TypeAlias, assert_never, cast

import pglast
from pglast import ast, enums

from htap.sql.ast.expression import (
    BinaryExpression,
    ColumnReference,
    Constant,
    Expression,
    FunctionCall,
    UnaryExpression,
)
from htap.sql.ast.from_clause import AliasedTable, FromClause, Join, JoinType
from htap.sql.ast.statement import (
    CreateStatement,
    DropRemoveType,
    DropStatement,
    SelectStatement,
    Statement,
    TargetList,
)
from htap.sql.ast.where_clause import CompoundPredicate, SimplePredicate, WhereClause
from htap.sql.operator import BinaryOperator, LogicalOperator, UnaryOperator
from htap.sql.schema import (
    Column,
    ColumnConstraint,
    DataType,
    RelationshipGraph,
    Schema,
    Table,
)

# Type aliases

ValueAst: TypeAlias = ast.Integer | ast.Float | ast.Boolean | ast.String
ExpressionAst: TypeAlias = (
    ast.A_Expr | ast.A_Const | ast.ColumnRef | ast.NullTest | ast.FuncCall
)
StatementAst: TypeAlias = (
    ast.CreateStmt
    | ast.DropStmt
    | ast.SelectStmt
    | ast.InsertStmt
    | ast.UpdateStmt
    | ast.DeleteStmt
)
FromClauseAst: TypeAlias = ast.JoinExpr | ast.RangeVar
WhereClauseAst: TypeAlias = ast.BoolExpr | ExpressionAst


def parse_sql(sql: str) -> Sequence[Statement]:
    """
    Parses a SQL string and returns the corresponding SQL statements.

    Raises a `ValueError` if the given input cannot be parsed as SQL,
    or if it contains some features not yet supported by this parser.

    Some of these are as follows:
    - `count(*)`
    - Aliases in SELECT target list (e.g., `SELECT price AS p FROM ...`)

    However, for some DDL statements, the parser may still return
    `Statement` objects that do not fully capture the semantics
    of the original SQL statements.

    E.g., `CONSTRAINT` inside a `CREATE` statement will be ignored.
    """
    try:
        raw_statements = pglast.parse_sql(sql)
        return [_parse_raw_statement(raw_statement) for raw_statement in raw_statements]
    except pglast.Error as exc:
        raise ValueError("Input string cannot be parsed") from exc
    except Exception as exc:
        raise ValueError("Error when parsing input SQL") from exc


def parse_schema(sql: str) -> Schema:
    """
    Parses a SQL string into a schema definition, i.e. SQL statements of
    table definitions (and optionally, drop statements).

    Raises a `ValueError` if the given SQL string is not a valid schema.
    """
    create_statements: list[CreateStatement] = []
    for statement in parse_sql(sql):
        match statement:
            case CreateStatement():
                create_statements.append(statement)
            case DropStatement():
                pass
            case _:
                raise ValueError("Schema should not contain any other statements")

    tables = [statement.table for statement in create_statements]
    tables_by_name = {table.name: table for table in tables}

    relationships = [
        (
            (from_table := statement.table),
            from_table.get_column(from_column_name),
            (to_table := tables_by_name[to_table_name]),
            to_table.get_column(to_column_name),
        )
        for statement in create_statements
        for (
            from_column_name,
            (to_table_name, to_column_name),
        ) in statement.fk_references.items()
    ]

    return Schema(
        tables=tables,
        relationships=RelationshipGraph(tables=tables, relationships=relationships),
    )


def _parse_raw_statement(node: ast.RawStmt) -> Statement:
    return _parse_statement(cast(StatementAst, node.stmt))


def _parse_statement(node: StatementAst) -> Statement:
    match node:
        case ast.CreateStmt():
            # NOTE: `element` can also be `ast.Constraint` object, but ignore for now
            # E.g., `CONSTRAINT joint_pkey PRIMARY KEY (a, b)`
            column_defs = [
                element
                for element in cast(tuple[ast.ColumnDef, ...], node.tableElts)
                if isinstance(element, ast.ColumnDef)
            ]

            return CreateStatement(
                table=Table(
                    name=cast(str, cast(ast.RangeVar, node.relation).relname),
                    columns=[
                        _parse_column_def(column_def) for column_def in column_defs
                    ],
                ),
                fk_references=_parse_fk_references(column_defs),
            )

        case ast.DropStmt():
            return DropStatement(
                remove_type=_parse_drop_remove_type(
                    cast(enums.parsenodes.ObjectType, node.removeType)
                ),
                is_cascade=(
                    cast(enums.parsenodes.DropBehavior, node.behavior)
                    is enums.parsenodes.DropBehavior.DROP_CASCADE
                ),
                is_missing_ok=cast(bool, node.missing_ok),
                objects=[
                    _parse_qualified_string(name)
                    for name in cast(tuple[tuple[ast.String, ...]], node.objects)
                ],
            )

        case ast.SelectStmt():
            target_list = _parse_target_list(
                cast(tuple[ast.ResTarget, ...], node.targetList)
            )

            from_clause = (
                _parse_from_clause(cast(tuple[FromClauseAst, ...], node.fromClause))
                if node.fromClause is not None
                else None
            )

            where_clause = (
                _parse_where_clause(cast(WhereClauseAst, node.whereClause))
                if node.whereClause is not None
                else None
            )

            return SelectStatement(
                target_list=target_list,
                from_clause=from_clause,
                where_clause=where_clause,
            )

        case _:
            raise NotImplementedError


def _parse_column_def(node: ast.ColumnDef) -> Column:
    constraints = {
        cast(enums.parsenodes.ConstrType, constraint.contype)
        for constraint in (
            cast(tuple[ast.Constraint, ...], node.constraints)
            if node.constraints is not None
            else ()
        )
    }
    type_data = cast(ast.TypeName, node.typeName)

    return Column(
        name=cast(str, node.colname),
        dtype=DataType.from_str(
            _parse_qualified_string(cast(tuple[ast.String, ...], type_data.names))
        ),
        constraint=ColumnConstraint(
            is_primary_key=enums.parsenodes.ConstrType.CONSTR_PRIMARY in constraints,
            is_not_null=enums.parsenodes.ConstrType.CONSTR_NOTNULL in constraints,
            is_unique=enums.parsenodes.ConstrType.CONSTR_UNIQUE in constraints,
            max_char_length=(
                _parse_integer(
                    cast(
                        ast.Integer,
                        cast(tuple[ast.A_Const, ...], type_data.typmods)[0].val,
                    )
                )
                if type_data.typmods is not None
                else None
            ),
        ),
    )


def _parse_fk_references(
    column_defs: Sequence[ast.ColumnDef],
) -> Mapping[str, tuple[str, str]]:
    fk_references: dict[str, tuple[str, str]] = {}

    for column_def in column_defs:
        if column_def.constraints is None:
            continue

        constraints = [
            constraint
            for constraint in cast(tuple[ast.Constraint, ...], column_def.constraints)
            if constraint.contype is enums.parsenodes.ConstrType.CONSTR_FOREIGN
        ]

        # There can only be at most one FK reference per column
        assert len(constraints) <= 1

        if len(constraints) == 0:
            continue

        fk_constraint = constraints[0]
        assert fk_constraint.pktable is not None
        assert fk_constraint.pk_attrs is not None

        column_name = cast(str, column_def.colname)
        fk_table = cast(str, cast(ast.RangeVar, fk_constraint.pktable).relname)
        fk_column = _parse_string(
            cast(tuple[ast.String, ...], fk_constraint.pk_attrs)[0]
        )

        fk_references[column_name] = (fk_table, fk_column)

    return fk_references


def _parse_target_list(nodes: Sequence[ast.ResTarget]) -> TargetList:
    return TargetList(
        targets=[_parse_expression(cast(ExpressionAst, node.val)) for node in nodes]
    )


def _parse_from_clause(nodes: Sequence[FromClauseAst]) -> FromClause:
    # Invariant from the parser
    assert len(nodes) > 0

    # Case 1: Cross joins (eg. FROM a, b)
    if len(nodes) > 1:
        return Join(
            join_type=JoinType.CROSS_JOIN,
            left=_parse_from_clause(nodes[:-1]),
            right=_parse_from_clause(nodes[-1:]),
            predicate=None,
        )

    # Case 2: Joins with explicit `JOIN` keyword
    match node := nodes[0]:
        case ast.JoinExpr():
            assert node.quals is not None

            join_type = cast(enums.nodes.JoinType, node.jointype)
            left_arg = cast(FromClauseAst, node.larg)
            right_arg = cast(FromClauseAst, node.rarg)
            join_predicate = cast(ast.A_Expr, node.quals)

            return Join(
                join_type=_parse_join_type(join_type),
                left=_parse_from_clause((left_arg,)),
                right=_parse_from_clause((right_arg,)),
                predicate=_parse_expression(join_predicate),
            )

        case ast.RangeVar():
            table_name = cast(str, node.relname)
            table_alias = (
                cast(str, cast(ast.Alias, node.alias).aliasname)
                if node.alias is not None
                else None
            )

            return AliasedTable(name=table_name, alias=table_alias)

        case _:
            assert_never(node)


def _parse_where_clause(node: WhereClauseAst) -> WhereClause:
    match node:
        case ast.BoolExpr():
            operator = cast(enums.primnodes.BoolExprType, node.boolop)
            children = cast(tuple[WhereClauseAst, ...], node.args)

            return CompoundPredicate(
                operator=_parse_boolean_operator(operator),
                children=[_parse_where_clause(node) for node in children],
            )

        # NOTE: This is not exhaustive (e.g., it does not handle subqueries)
        case (
            ast.A_Expr()
            | ast.A_Const()
            | ast.ColumnRef()
            | ast.NullTest()
            | ast.FuncCall()
        ):
            return SimplePredicate(expression=_parse_expression(node))

        case _:
            assert_never(node)


def _parse_expression(node: ExpressionAst) -> Expression:
    match node:
        case ast.A_Expr():
            return _parse_a_expression(node)

        case ast.A_Const():
            # Note: We don't handle `node.isnull` for now
            return Constant(value=_parse_value(cast(ValueAst, node.val)))

        case ast.ColumnRef():
            values = [
                _parse_string(field)
                for field in cast(tuple[ast.String, ...], node.fields)
            ]

            if len(values) == 1:
                return ColumnReference(table_alias=None, column_name=values[0])

            if len(values) == 2:
                return ColumnReference(table_alias=values[0], column_name=values[1])

            raise RuntimeError("Should not reach here")

        case ast.NullTest():
            null_test_type = cast(enums.primnodes.NullTestType, node.nulltesttype)
            operand = cast(ExpressionAst, node.arg)

            return UnaryExpression(
                operator=_parse_null_test_type(null_test_type),
                operand=_parse_expression(operand),
            )

        case ast.FuncCall():
            func_name = cast(tuple[ast.String, ...], node.funcname)
            args = cast(tuple[ExpressionAst, ...] | None, node.args)

            # NOTE: We don't handle function calls without arguments for now
            # This includes `count(*)`
            if args is None:
                raise NotImplementedError

            return FunctionCall(
                func_name=_parse_qualified_string(func_name),
                args=[_parse_expression(arg) for arg in args],
            )

        case _:
            assert_never(node)


def _parse_a_expression(node: ast.A_Expr) -> Expression:
    kind = cast(enums.parsenodes.A_Expr_Kind, node.kind)
    operator_symbols = cast(tuple[ast.String, ...], node.name)

    # Case 1: Unary operations
    if kind is enums.parsenodes.A_Expr_Kind.AEXPR_OP and node.lexpr is None:
        right_expr = cast(ExpressionAst, node.rexpr)

        return UnaryExpression(
            operator=UnaryOperator.from_symbol(
                _parse_qualified_string(operator_symbols)
            ),
            operand=_parse_expression(right_expr),
        )

    # Case 2: Binary operations with non-sequence RHS
    if (
        # Using `is` instead of `in` allows the type checker
        # to narrow the possible values
        kind is enums.parsenodes.A_Expr_Kind.AEXPR_OP
        or kind is enums.parsenodes.A_Expr_Kind.AEXPR_LIKE
        or kind is enums.parsenodes.A_Expr_Kind.AEXPR_ILIKE
    ):
        left_expr = cast(ExpressionAst, node.lexpr)
        right_expr = cast(ExpressionAst, node.rexpr)

        match kind:
            case enums.parsenodes.A_Expr_Kind.AEXPR_OP:
                operator = BinaryOperator.from_symbol(
                    _parse_qualified_string(operator_symbols)
                )
            case enums.parsenodes.A_Expr_Kind.AEXPR_LIKE:
                operator = BinaryOperator.LIKE
            case enums.parsenodes.A_Expr_Kind.AEXPR_ILIKE:
                operator = BinaryOperator.ILIKE
            case _:
                assert_never(kind)

        return BinaryExpression(
            operator=operator,
            left=_parse_expression(left_expr),
            right=_parse_expression(right_expr),
        )

    # Case 3: Binary operations with sequence RHS
    left_expr = cast(ExpressionAst, node.lexpr)
    right_exprs = cast(tuple[ExpressionAst, ...], node.rexpr)

    match kind:
        case enums.parsenodes.A_Expr_Kind.AEXPR_IN:
            operator = BinaryOperator.IN
        case enums.parsenodes.A_Expr_Kind.AEXPR_BETWEEN:
            operator = BinaryOperator.BETWEEN
        case enums.parsenodes.A_Expr_Kind.AEXPR_NOT_BETWEEN:
            operator = BinaryOperator.NOT_BETWEEN
        case _:
            raise NotImplementedError

    return BinaryExpression(
        operator=operator,
        left=_parse_expression(left_expr),
        right=[_parse_expression(expr) for expr in right_exprs],
    )


def _parse_value(node: ValueAst) -> int | float | str | bool:
    match node:
        case ast.Integer():
            return _parse_integer(node)
        case ast.Float():
            return _parse_float(node)
        case ast.Boolean():
            return _parse_boolean(node)
        case ast.String():
            return _parse_string(node)
        case _:
            assert_never(node)


def _parse_integer(node: ast.Integer) -> int:
    return cast(int, node.ival)


def _parse_float(node: ast.Float) -> float:
    return cast(float, node.fval)


def _parse_string(node: ast.String) -> str:
    return cast(str, node.sval)


def _parse_boolean(node: ast.Boolean) -> bool:
    return cast(bool, node.boolval)


def _parse_qualified_string(nodes: Sequence[ast.String]) -> str:
    return ".".join(_parse_string(node) for node in nodes)


def _parse_drop_remove_type(remove_type: enums.parsenodes.ObjectType) -> DropRemoveType:
    match remove_type:
        case enums.parsenodes.ObjectType.OBJECT_TABLE:
            return DropRemoveType.TABLE
        case enums.parsenodes.ObjectType.OBJECT_COLUMN:
            return DropRemoveType.COLUMN
        case _:
            raise NotImplementedError


def _parse_join_type(join_type: enums.nodes.JoinType) -> JoinType:
    match join_type:
        case enums.nodes.JoinType.JOIN_INNER:
            return JoinType.INNER_JOIN
        case enums.nodes.JoinType.JOIN_LEFT:
            return JoinType.LEFT_OUTER_JOIN
        case enums.nodes.JoinType.JOIN_RIGHT:
            return JoinType.RIGHT_OUTER_JOIN
        case enums.nodes.JoinType.JOIN_FULL:
            return JoinType.FULL_OUTER_JOIN
        case _:
            # Postgres defines other types of joins
            raise NotImplementedError


def _parse_boolean_operator(operator: enums.primnodes.BoolExprType) -> LogicalOperator:
    match operator:
        case enums.primnodes.BoolExprType.AND_EXPR:
            return LogicalOperator.AND
        case enums.primnodes.BoolExprType.OR_EXPR:
            return LogicalOperator.OR
        case enums.primnodes.BoolExprType.NOT_EXPR:
            return LogicalOperator.NOT
        case _:
            assert_never(operator)


def _parse_null_test_type(
    null_test_type: enums.primnodes.NullTestType,
) -> UnaryOperator:
    match null_test_type:
        case enums.primnodes.NullTestType.IS_NULL:
            return UnaryOperator.IS_NULL
        case enums.primnodes.NullTestType.IS_NOT_NULL:
            return UnaryOperator.IS_NOT_NULL
        case _:
            assert_never(null_test_type)
