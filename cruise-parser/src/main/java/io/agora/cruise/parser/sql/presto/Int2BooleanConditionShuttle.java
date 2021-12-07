package io.agora.cruise.parser.sql.presto;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Int2BooleanConditionShuttle. if condition contains case when in and or operator, cast inter value
 * to boolean value
 */
public class Int2BooleanConditionShuttle extends SqlShuttle {

    @Override
    public @Nullable SqlNode visit(final SqlCall call) {
        if (call.getKind() == SqlKind.SELECT) {
            final SqlNode where = ((SqlSelect) call).getWhere();
            if (where != null) {
                where.accept(this);
            }
            return call;
        } else if (relatedKind(call)) {
            final List<SqlNode> operand = call.getOperandList();
            for (final SqlNode node : operand) {
                if (node instanceof SqlCase || relatedKind(call)) {
                    node.accept(this);
                }
            }
            return call;
        } else if (call.getKind() == SqlKind.CASE) {
            final SqlCase sqlCase = (SqlCase) call;
            final SqlNodeList thenOperands = sqlCase.getThenOperands();
            for (int i = 0; i < thenOperands.size(); i++) {
                if (thenOperands.get(i) instanceof SqlNumericLiteral) {
                    final boolean isTrue =
                            ((SqlNumericLiteral) thenOperands.get(i)).toValue().equals("1");
                    thenOperands.set(
                            i,
                            SqlLiteral.createBoolean(
                                    isTrue, thenOperands.get(i).getParserPosition()));
                }
            }
            if (sqlCase.getElseOperand() instanceof SqlNumericLiteral) {
                final boolean isTrue =
                        ((SqlNumericLiteral) sqlCase.getElseOperand()).toValue().equals("1");
                sqlCase.setOperand(
                        3,
                        SqlLiteral.createBoolean(
                                isTrue, sqlCase.getElseOperand().getParserPosition()));
            }
            return call;
        } else {
            return super.visit(call);
        }
    }

    /**
     * get related call kind.
     *
     * @param call call
     * @return boolean related
     */
    protected boolean relatedKind(final SqlCall call) {
        return call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR;
    }
}
