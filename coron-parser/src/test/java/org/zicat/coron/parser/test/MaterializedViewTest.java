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
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.coron.parser.SqlNodeUtils;

import java.util.Set;

/** MaterializedViewTest. */
public class MaterializedViewTest extends TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewTest.class);

    public MaterializedViewTest() {}

    @Test
    public void testView2() throws SqlParseException {
        String viewQuerySql = "select sum(c), count(*) as s_c from test_db.test_table";
        String viewTableName = "test_db.testView2";
        addMaterializedView(viewTableName, viewQuerySql);
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(
                        "select sum(c) as s_c from test_db.test_table having count(*) > 1",
                        SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        final RelNode optNode = materializedViewOpt(relNode).left;
        final Set<String> queryTables = TableRelShuttleImpl.tables(optNode);
        Assert.assertTrue(queryTables.contains(viewTableName));
    }

    @Test
    public void testAddView() throws SqlParseException {
        String viewQuerySql = "select a, sum(c) as s_c from test_db.test_table group by a";
        String viewTableName = "test_db.materialized_view";
        addMaterializedView(viewTableName, viewQuerySql);
        String sql1 = "select * from test_db.materialized_view";
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(sql1, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());

        final SqlNode sqlNode2 =
                SqlNodeUtils.toSqlNode(viewQuerySql, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);
        final RelNode optRelNode = materializedViewOpt(relNode2).left;
        final Set<String> queryTables = TableRelShuttleImpl.tables(optRelNode);
        Assert.assertTrue(queryTables.contains(viewTableName));
    }

    @Test
    public void testView3() throws SqlParseException {
        String viewQuerySql = "select a,b,sum(c) from test_db.test_table group by a,b";
        String viewTableName = "test_db.testView2";
        addMaterializedView(viewTableName, viewQuerySql);
        final SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(
                        "select a,sum(c) from test_db.test_table group by a",
                        SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        final RelNode optNode = materializedViewOpt(relNode).left;
        final Set<String> queryTables = TableRelShuttleImpl.tables(optNode);
        Assert.assertTrue(queryTables.contains(viewTableName));
    }
}
