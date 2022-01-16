/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.validate;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getCondition;
import static org.apache.calcite.util.Static.RESOURCE;

/** ClickHouseSqlValidatorImpl. */
public class CoronSqlValidatorImpl extends SqlValidatorImpl {

    protected SqlTypeFactoryImpl sqlTypeFactory;

    @Override
    public RelDataType getValidatedNodeType(SqlNode node) {
        if (node instanceof SqlCall && ((SqlCall) node).getOperator().getKind() == SqlKind.OVER) {
            SqlNode operand0 = ((SqlCall) node).getOperandList().get(0);
            if (operand0 instanceof SqlCall
                    && ((SqlCall) operand0).getOperator().getKind() == SqlKind.RANK) {
                return sqlTypeFactory.createSqlType(SqlTypeName.BIGINT);
            }
        }
        if (!(node instanceof SqlIdentifier)) {
            return super.getValidatedNodeType(node);
        }
        SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
        if (sqlIdentifier.names.size() != 1) {
            return super.getValidatedNodeType(node);
        }

        switch (sqlIdentifier.getSimple()) {
            case "STRING":
                return sqlTypeFactory.createSqlType(SqlTypeName.VARCHAR);
            case "INT64":
                return sqlTypeFactory.createSqlType(SqlTypeName.BIGINT);
            case "INT32":
                return sqlTypeFactory.createSqlType(SqlTypeName.INTEGER);
            default:
                return super.getValidatedNodeType(node);
        }
    }

    /**
     * Creates a validator.
     *
     * @param opTab Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory Type factory
     * @param config Config
     */
    protected CoronSqlValidatorImpl(
            SqlTypeFactoryImpl sqlTypeFactory,
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config) {
        super(opTab, catalogReader, typeFactory, config);
        this.sqlTypeFactory = sqlTypeFactory;
    }

    @Override
    public boolean isAggregate(SqlNode selectNode) {
        return true;
    }

    public static SqlValidatorWithHints newValidator(
            SqlTypeFactoryImpl sqlTypeFactory,
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config) {
        return new CoronSqlValidatorImpl(sqlTypeFactory, opTab, catalogReader, typeFactory, config);
    }

    @Override
    public SqlNode expandGroupByOrHavingExpr(
            SqlNode expr, SqlValidatorScope scope, SqlSelect select, boolean havingExpression) {
        final Expander expander = new ExtendedExpander(this, scope, select, expr, havingExpression);
        SqlNode newExpr = expander.go(expr);
        if (expr != newExpr) {
            setOriginal(newExpr, expr);
        }
        return newExpr;
    }

    private static SqlNode expandCommonColumn(
            SqlSelect sqlSelect,
            SqlNode selectItem,
            @Nullable SelectScope scope,
            SqlValidatorImpl validator) {
        if (!(selectItem instanceof SqlIdentifier)) {
            return selectItem;
        }

        final SqlNode from = sqlSelect.getFrom();
        if (!(from instanceof SqlJoin)) {
            return selectItem;
        }

        final SqlIdentifier identifier = (SqlIdentifier) selectItem;
        if (!identifier.isSimple()) {
            if (!validator.config().sqlConformance().allowQualifyingCommonColumn()) {
                validateQualifiedCommonColumn((SqlJoin) from, identifier, scope, validator);
            }
            return selectItem;
        }

        return expandExprFromJoin((SqlJoin) from, identifier, scope);
    }

    private static SqlNode expandExprFromJoin(
            SqlJoin join, SqlIdentifier identifier, @Nullable SelectScope scope) {
        if (join.getConditionType() != JoinConditionType.USING) {
            return identifier;
        }

        for (String name : SqlIdentifier.simpleNames((SqlNodeList) getCondition(join))) {
            if (identifier.getSimple().equals(name)) {
                final List<SqlNode> qualifiedNode = new ArrayList<>();
                for (ScopeChild child : requireNonNull(scope, "scope").children) {
                    if (child.namespace.getRowType().getFieldNames().indexOf(name) >= 0) {
                        final SqlIdentifier exp =
                                new SqlIdentifier(
                                        ImmutableList.of(child.name, name),
                                        identifier.getParserPosition());
                        qualifiedNode.add(exp);
                    }
                }

                assert qualifiedNode.size() == 2;
                final SqlNode finalNode =
                        SqlStdOperatorTable.AS.createCall(
                                SqlParserPos.ZERO,
                                SqlStdOperatorTable.COALESCE.createCall(
                                        SqlParserPos.ZERO,
                                        qualifiedNode.get(0),
                                        qualifiedNode.get(1)),
                                new SqlIdentifier(name, SqlParserPos.ZERO));
                return finalNode;
            }
        }

        // Only need to try to expand the expr from the left input of join
        // since it is always left-deep join.
        final SqlNode node = join.getLeft();
        if (node instanceof SqlJoin) {
            return expandExprFromJoin((SqlJoin) node, identifier, scope);
        } else {
            return identifier;
        }
    }

    private static void validateQualifiedCommonColumn(
            SqlJoin join,
            SqlIdentifier identifier,
            @Nullable SelectScope scope,
            SqlValidatorImpl validator) {
        List<String> names = validator.usingNames(join);
        if (names == null) {
            // Not USING or NATURAL.
            return;
        }

        requireNonNull(scope, "scope");
        // First we should make sure that the first component is the table name.
        // Then check whether the qualified identifier contains common column.
        for (ScopeChild child : scope.children) {
            if (Objects.equals(child.name, identifier.getComponent(0).toString())) {
                if (names.contains(identifier.getComponent(1).toString())) {
                    throw validator.newValidationError(
                            identifier,
                            RESOURCE.disallowsQualifyingCommonColumn(identifier.toString()));
                }
            }
        }

        // Only need to try to validate the expr from the left input of join
        // since it is always left-deep join.
        final SqlNode node = join.getLeft();
        if (node instanceof SqlJoin) {
            validateQualifiedCommonColumn((SqlJoin) node, identifier, scope, validator);
        }
    }
    /**
     * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing usages of
     * aliases or ordinals with the underlying expression.
     */
    static class ExtendedExpander extends Expander {
        final SqlSelect select;
        final SqlNode root;
        final boolean havingExpr;

        ExtendedExpander(
                SqlValidatorImpl validator,
                SqlValidatorScope scope,
                SqlSelect select,
                SqlNode root,
                boolean havingExpr) {
            super(validator, scope);
            this.select = select;
            this.root = root;
            this.havingExpr = havingExpr;
        }

        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            if (id.isSimple()
                    && (havingExpr
                            ? validator.config().sqlConformance().isHavingAlias()
                            : validator.config().sqlConformance().isGroupByAlias())) {
                String name = id.getSimple();
                SqlNode expr = null;
                final SqlNameMatcher nameMatcher = validator.getCatalogReader().nameMatcher();
                int n = 0;
                for (SqlNode s : SqlNonNullableAccessors.getSelectList(select)) {
                    final String alias = SqlValidatorUtil.getAlias(s, -1);
                    if (s.getKind() == SqlKind.AS) {
                        SqlCall call = (SqlCall) s;
                        SqlNode function = call.getOperandList().get(0);
                        final AtomicBoolean found = new AtomicBoolean(false);
                        function.accept(
                                new SqlShuttle() {
                                    @Override
                                    public @Nullable SqlNode visit(SqlIdentifier id) {
                                        if (id.toString().equals(alias)) {
                                            found.set(true);
                                        }
                                        return super.visit(id);
                                    }
                                });
                        if (found.get()) {
                            continue;
                        }
                    }
                    if (alias != null && nameMatcher.matches(alias, name)) {
                        expr = s;
                        n++;
                    }
                }
                if (n == 0) {
                    return super.visit(id);
                } else if (n > 1) {
                    // More than one column has this alias.
                    throw validator.newValidationError(id, RESOURCE.columnAmbiguous(name));
                }
                if (havingExpr && validator.isAggregate(root)) {
                    return super.visit(id);
                }
                expr = stripAs(expr);
                if (expr instanceof SqlIdentifier) {
                    SqlIdentifier sid = (SqlIdentifier) expr;
                    final SqlIdentifier fqId = getScope().fullyQualify(sid).identifier;
                    expr = expandDynamicStar(sid, fqId);
                }
                return expr;
            }
            if (id.isSimple()) {
                final SelectScope scope = validator.getRawSelectScope(select);
                SqlNode node = expandCommonColumn(select, id, scope, validator);
                if (node != id) {
                    return node;
                }
            }
            return super.visit(id);
        }

        @Override
        public @Nullable SqlNode visit(SqlLiteral literal) {
            if (havingExpr || !validator.config().sqlConformance().isGroupByOrdinal()) {
                return super.visit(literal);
            }
            boolean isOrdinalLiteral = literal == root;
            switch (root.getKind()) {
                case GROUPING_SETS:
                case ROLLUP:
                case CUBE:
                    if (root instanceof SqlBasicCall) {
                        List<SqlNode> operandList = ((SqlBasicCall) root).getOperandList();
                        for (SqlNode node : operandList) {
                            if (node.equals(literal)) {
                                isOrdinalLiteral = true;
                                break;
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
            if (isOrdinalLiteral) {
                switch (literal.getTypeName()) {
                    case DECIMAL:
                    case DOUBLE:
                        final int intValue = literal.intValue(false);
                        if (intValue >= 0) {
                            if (intValue < 1
                                    || intValue
                                            > SqlNonNullableAccessors.getSelectList(select)
                                                    .size()) {
                                throw validator.newValidationError(
                                        literal, RESOURCE.orderByOrdinalOutOfRange());
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            int ordinal = intValue - 1;
                            return SqlUtil.stripAs(
                                    SqlNonNullableAccessors.getSelectList(select).get(ordinal));
                        }
                        break;
                    default:
                        break;
                }
            }

            return super.visit(literal);
        }
    }

    private static class Expander extends SqlScopedShuttle {
        protected final SqlValidatorImpl validator;

        Expander(SqlValidatorImpl validator, SqlValidatorScope scope) {
            super(scope);
            this.validator = validator;
        }

        public SqlNode go(SqlNode root) {
            return requireNonNull(root.accept(this), () -> this + " returned null for " + root);
        }

        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            // First check for builtin functions which don't have
            // parentheses, like "LOCALTIME".
            final SqlCall call = validator.makeNullaryCall(id);
            if (call != null) {
                return call.accept(this);
            }
            final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;
            SqlNode expandedExpr = expandDynamicStar(id, fqId);
            validator.setOriginal(expandedExpr, id);
            return expandedExpr;
        }

        @Override
        protected SqlNode visitScoped(SqlCall call) {
            switch (call.getKind()) {
                case SCALAR_QUERY:
                case CURRENT_VALUE:
                case NEXT_VALUE:
                case WITH:
                    return call;
                default:
                    break;
            }
            // Only visits arguments which are expressions. We don't want to
            // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
            CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, false);
            call.getOperator().acceptCall(this, call, true, argHandler);
            final SqlNode result = argHandler.result();
            validator.setOriginal(result, call);
            return result;
        }

        protected SqlNode expandDynamicStar(SqlIdentifier id, SqlIdentifier fqId) {
            if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
                    && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
                // Convert a column ref into ITEM(*, 'col_name')
                // for a dynamic star field in dynTable's rowType.
                SqlNode[] inputs = new SqlNode[2];
                inputs[0] = fqId;
                inputs[1] =
                        SqlLiteral.createCharString(Util.last(id.names), id.getParserPosition());
                return new SqlBasicCall(SqlStdOperatorTable.ITEM, inputs, id.getParserPosition());
            }
            return fqId;
        }
    }
}
