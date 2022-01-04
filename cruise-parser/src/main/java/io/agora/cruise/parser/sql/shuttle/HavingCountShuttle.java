package io.agora.cruise.parser.sql.shuttle;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.Objects;

/** HavingCountShuttle. remove having count(1|*) > 0 */
@Deprecated
public class HavingCountShuttle extends SqlShuttle {

    @Override
    public @Nullable SqlNode visit(final SqlCall call) {
        if (call.getKind() != SqlKind.SELECT) {
            return super.visit(call);
        }
        final SqlSelect select = (SqlSelect) call;
        if (select.getHaving() == null || !(select.getHaving() instanceof SqlBasicCall)) {
            return super.visit(call);
        }
        final SqlBasicCall having = (SqlBasicCall) select.getHaving();
        if (having.getOperator().getKind() != SqlKind.GREATER_THAN) {
            return super.visit(call);
        }
        final SqlNode value = having.getOperands()[1];
        if (!(value instanceof SqlNumericLiteral)
                || !Objects.equals(((SqlNumericLiteral) value).getValue(), new BigDecimal(0))) {
            return super.visit(call);
        }
        final SqlNode functionNode = having.getOperands()[0];
        if (!(functionNode instanceof SqlBasicCall)) {
            return super.visit(call);
        }
        final SqlBasicCall functionCall = (SqlBasicCall) functionNode;
        if (!functionCall.getOperator().getName().toUpperCase().equals("COUNT")) {
            return super.visit(call);
        }
        final SqlNode functionParamNode = functionCall.getOperands()[0];
        if (!(functionParamNode.toString().equals("1")
                || functionParamNode.toString().equals("*"))) {
            return super.visit(call);
        }
        select.setHaving(null);
        return super.visit(call);
    }
}
