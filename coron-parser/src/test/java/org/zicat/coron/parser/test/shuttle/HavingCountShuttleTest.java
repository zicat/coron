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

package org.zicat.coron.parser.test.shuttle;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.coron.parser.SqlNodeUtils;
import org.zicat.coron.parser.sql.shuttle.HavingCountShuttle;

/** HavingCountShuttleTest. */
public class HavingCountShuttleTest {

    @Test
    public void test() throws SqlParseException {

        String querySql = "select * from table1 having count(1) > 0";
        String querySql2 = "select * from table1 having count(*) > 0";

        String expectSql = "SELECT *\nFROM `table1`";

        Assert.assertEquals(
                expectSql,
                SqlNodeUtils.toSqlNode(
                                querySql,
                                SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG,
                                new HavingCountShuttle())
                        .toString());
        Assert.assertEquals(
                expectSql,
                SqlNodeUtils.toSqlNode(
                                querySql2,
                                SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG,
                                new HavingCountShuttle())
                        .toString());

        String querySql3 = "select * from table1 having count(*) = 0";
        String expectSql3 = "SELECT *\nFROM `table1`\nHAVING COUNT(*) = 0";
        Assert.assertEquals(
                expectSql3,
                SqlNodeUtils.toSqlNode(
                                querySql3,
                                SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG,
                                new HavingCountShuttle())
                        .toString());
    }
}
