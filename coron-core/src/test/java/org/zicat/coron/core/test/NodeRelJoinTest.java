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

/** NodeRelJoinTest. */
public class NodeRelJoinTest extends NodeRelTest {

    public NodeRelJoinTest() {}

    @Test
    public void testJoin() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final String sql2 =
                "select t1.a from test_db.test_table t1 "
                        + "left join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";

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
    public void testJoin2() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String sql2 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.c ";

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
    public void testJoin3() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final String sql2 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final String expectSql =
                "SELECT test_table.a\n"
                        + "FROM test_db.test_table\n"
                        + "INNER JOIN test_db.test_table test_table0 ON test_table.a = test_table0.a AND test_table.b = test_table0.a";

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

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }

    @Test
    public void testJoin4() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String sql2 =
                "select t2.a as x from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String expectSql =
                "SELECT test_table.a, test_table0.a x\n"
                        + "FROM test_db.test_table\n"
                        + "INNER JOIN test_db.test_table test_table0 ON test_table.a = test_table0.a AND test_table.b = test_table0.b";

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

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }

    @Test
    public void testJoin5() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String sql2 =
                "select t2.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String expectSql =
                "SELECT *\n"
                        + "FROM test_db.test_table\n"
                        + "INNER JOIN test_db.test_table test_table0 ON test_table.a = test_table0.a AND test_table.b = test_table0.b";

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

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }
}
