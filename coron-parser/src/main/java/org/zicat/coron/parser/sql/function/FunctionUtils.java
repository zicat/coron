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

package org.zicat.coron.parser.sql.function;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlRegexpOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.Optionality;

/** FunctionUtils. */
public class FunctionUtils {

    public static SqlOperatorTable sqlOperatorTable;

    static {
        ListSqlOperatorTable listSqlOperatorTable = new ListSqlOperatorTable();
        listSqlOperatorTable.add(new CollectionDistinct());
        listSqlOperatorTable.add(SqlRegexpOperator.INSTANCE);
        sqlOperatorTable =
                SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance());
    }

    /** CollectionDistinct. */
    public static class CollectionDistinct extends SqlAggFunction {

        public CollectionDistinct() {
            super(
                    "collection_distinct",
                    null,
                    SqlKind.ARRAY_AGG,
                    ReturnTypes.VARCHAR_2000,
                    null,
                    OperandTypes.family(SqlTypeFamily.ANY),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false,
                    Optionality.OPTIONAL);
        }
    }
}
