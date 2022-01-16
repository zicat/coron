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

package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.*;

public class SqlRegexpOperator extends SqlSpecialOperator {

    public static final SqlOperator INSTANCE = new SqlRegexpOperator();

    public SqlRegexpOperator() {
        this("REGEXP", SqlKind.OTHER, 32, false);
    }

    public SqlRegexpOperator(String name, SqlKind kind, int prec, boolean leftAssoc) {
        super(
                name,
                kind,
                prec,
                leftAssoc,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.STRING_SAME_SAME_SAME);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.between(2, 3);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList("", "");
        call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());
        writer.sep(getName());
        call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
        writer.endList(frame);
    }

    @Override
    public ReduceResult reduceExpr(final int opOrdinal, TokenSequence list) {
        // Example:
        //   a LIKE b || c ESCAPE d || e AND f
        // |  |    |      |      |      |
        //  exp0    exp1          exp2
        SqlNode exp0 = list.node(opOrdinal - 1);
        SqlOperator op = list.op(opOrdinal);
        assert op instanceof SqlRegexpOperator;
        SqlNode exp1 = SqlParserUtil.toTreeEx(list, opOrdinal + 1, getRightPrec(), SqlKind.ESCAPE);
        SqlNode exp2 = null;
        if ((opOrdinal + 2) < list.size()) {
            if (list.isOp(opOrdinal + 2)) {
                final SqlOperator op2 = list.op(opOrdinal + 2);
                if (op2.getKind() == SqlKind.ESCAPE) {
                    exp2 =
                            SqlParserUtil.toTreeEx(
                                    list, opOrdinal + 3, getRightPrec(), SqlKind.ESCAPE);
                }
            }
        }
        final SqlNode[] operands;
        final int end;
        if (exp2 != null) {
            operands = new SqlNode[] {exp0, exp1, exp2};
            end = opOrdinal + 4;
        } else {
            operands = new SqlNode[] {exp0, exp1};
            end = opOrdinal + 2;
        }
        SqlCall call = createCall(SqlParserPos.sum(operands), operands);
        return new ReduceResult(opOrdinal - 1, end, call);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        if (!OperandTypes.STRING_SAME_SAME.checkOperandTypes(callBinding, throwOnFailure)) {
            return false;
        }
        return SqlTypeUtil.isCharTypeComparable(
                callBinding, callBinding.operands(), throwOnFailure);
    }
}
