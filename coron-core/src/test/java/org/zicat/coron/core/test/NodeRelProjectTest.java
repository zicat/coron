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

package org.zicat.coron.core.test;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.coron.core.NodeUtils;
import org.zicat.coron.core.ResultNode;

/** NodeRelProjectTest. */
public class NodeRelProjectTest extends NodeRelTest {

    public NodeRelProjectTest() {}

    @Test
    public void testProject() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT a, b  FROM test_db.test_table";
        final String expectSql = "SELECT a, b, ABS(a) c\nFROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);

        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject2() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT c, b  FROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);
        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testProject3() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT abs(d) as c, b  FROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);

        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());
        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testProject4() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String expectSql = "SELECT b, ABS(a) c\nFROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);

        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject5() throws SqlParseException {

        final String sql1 = "SELECT * FROM test_db.test_table";
        final String sql2 = "SELECT abs(a) as x, b FROM test_db.test_table";
        final String expectSql = "SELECT a, b, c, d, ABS(a) x\nFROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);

        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject6() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as f, abs(b) as g FROM test_db.test_table";
        final String sql2 = "SELECT abs(b) as f, abs(a) as g FROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);
        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testProject7() throws SqlParseException {

        final String sql1 = "SELECT a as f, b as g FROM test_db.test_table";
        final String sql2 = "SELECT * FROM test_db.test_table";
        final String expectSql = "SELECT a, b, c, d, a f, b g\nFROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);

        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject8() throws SqlParseException {
        final String sql1 =
                "select * from(select a, b "
                        + ",row_number() over (partition by a order by b asc) as row_num"
                        + ",max(c) over (partition by a)  as max_c "
                        + "from test_db.test_table) t where t.row_num = 1";

        final String sql2 =
                "select * from(select a, b "
                        + ",row_number() over (partition by a order by b asc) as row_num"
                        + ",max(c) over (partition by a)  as max_c "
                        + "from test_db.test_table) t where t.row_num <= 2";

        final String expectSql =
                "SELECT *\n"
                        + "FROM (SELECT a, b, MAX(c) OVER (PARTITION BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) max_c, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b NULLS LAST) row_num\n"
                        + "FROM test_db.test_table) t\n"
                        + "WHERE row_num = 1 OR row_num <= 2";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);
        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2),
                        NodeUtils.createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }
}
