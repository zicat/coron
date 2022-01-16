/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.parser.sql.shuttle;

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
