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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.coron.parser.SqlNodeUtils;

/** SqlToRelConverterToolTest. */
public class SqlToRelConverterUtilsTest extends TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlToRelConverterUtilsTest.class);

    public SqlToRelConverterUtilsTest() {}

    @Test
    public void test() throws SqlParseException {
        final String querySql =
                "SELECT a, b, sum(c) from test_db.test_table where a > '10' group by a, b";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testUdf() throws SqlParseException {
        final String querySql =
                "SELECT a, my_udf(b), collection_distinct(c) from test_db.test_table where a > '10' group by a, b";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testOverWindow() throws SqlParseException {
        final String querySql =
                "select * from(select a, b "
                        + ",row_number() over (partition by a order by b asc) as row_num"
                        + ",max(c) over (partition by a)  as max_c "
                        + "from test_db.test_table) t where t.row_num = 1";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testDistinct() throws SqlParseException {
        final String querySql =
                "select a, count(distinct if(c > 0, b, a)) as c1 from test_db.test_table group by a";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testGroupSet() throws SqlParseException {
        final String querySql =
                "select a, b, c, count(distinct if(c > 0, b, a)) as c1 from test_db.test_table group by  b,c grouping sets((),(a,b),(a,c))";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testGroupAlias() throws SqlParseException {
        final String querySql =
                "select case when b = '1' then 't' else x end as x from test_db.test_table group by case when b = '1' then 't' else x end";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testWindowRank() throws SqlParseException {
        final String querySql =
                "select a,c, case when b = '1' then 't' else x end as x, RANK() OVER (PARTITION BY a ORDER BY c DESC) AS highest_ver_rank"
                        + " from test_db.test_table group by  a,c, case when b = '1' then 't' else x end";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());
    }

    @Test
    public void testRegexp() throws SqlParseException {
        final String querySql = "select * from test_db.test_table WHERE b REGEXP '^[0-9]' ";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(querySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        final String expectSql = "SELECT *\nFROM test_db.test_table\nWHERE b REGEXP '^[0-9]'";
        Assert.assertNotNull(relNode);
        Assert.assertEquals(expectSql, toSql(relNode));
    }
}
