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

package org.zicat.coron.parser.test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.coron.parser.SqlNodeUtils;

/** SqlNodeToolTest. */
public class SqlNodeToolTest extends TestBase {

    String querySql = "SELECT a, b, sum(c) from t1 where a > '10' group by a, b";

    String expectQuerySql =
            "SELECT a, b, SUM(c)\n" + "FROM t1\n" + "WHERE a > '10'\n" + "GROUP BY a, b";

    public SqlNodeToolTest() {}

    @Test
    public void testQuerySql() throws SqlParseException {
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        Assert.assertEquals(expectQuerySql, SqlNodeUtils.toSql(sqlNode));
    }

    @Test
    public void testDDLSql() throws SqlParseException {
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(ddl1, SqlNodeUtils.DEFAULT_DDL_PARSER_CONFIG);
        Assert.assertEquals(ddl1, SqlNodeUtils.toSql(sqlNode));
    }
}
