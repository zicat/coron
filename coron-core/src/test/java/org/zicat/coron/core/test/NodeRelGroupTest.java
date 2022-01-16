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
import org.junit.Test;
import org.zicat.coron.core.NodeUtils;
import org.zicat.coron.core.ResultNode;

/** NodeRelGroupTest. */
public class NodeRelGroupTest extends NodeRelTest {

    public NodeRelGroupTest() {}

    @Test
    public void testGroupBy() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, c as dd, sum(d) from test_db.test_table WHERE c < 1000 group by a, c";
        final String expectSql =
                "SELECT a, b, c, d\nFROM test_db.test_table\nWHERE c < 5000 OR c < 1000";

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
    public void testGroupBy2() throws SqlParseException {
        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(d) from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql =
                "SELECT a, b, c, d\nFROM test_db.test_table\nWHERE c < 5000 OR c < 1000";

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
    public void testGroupBy3() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) as p from test_db.test_table WHERE a < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, max(c) as t from test_db.test_table WHERE a < 1000 group by a, b";
        final String expectSql =
                "SELECT a aa, b bb, a cc, b dd, SUM(c) p, MAX(c) t\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE a < 5000 OR a < 1000\n"
                        + "GROUP BY a, b";

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
    public void testGroupBy4() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE a < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(c) from test_db.test_table WHERE a < 1000 group by a, b";
        final String expectSql =
                "SELECT SUM(c), a aa, b bb, a cc, b dd\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE a < 5000 OR a < 1000\n"
                        + "GROUP BY a, b";

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
    public void testGroupBy5() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) as p from test_db.test_table WHERE b < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(c) as t from test_db.test_table WHERE b < 1000 group by a, b";
        final String expectSql =
                "SELECT a aa, b bb, a cc, b dd, SUM(c) p, SUM(c) t\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE b < 5000 OR b < 1000\n"
                        + "GROUP BY a, b";

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
    public void testGroupBy6() throws SqlParseException {

        final String sql1 =
                "select a as aa, sum(c) as p from test_db.test_table WHERE a < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b, sum(c) as t from test_db.test_table WHERE a > 1000 group by a, b";
        final String expectSql =
                "SELECT a aa, b, a cc, SUM(c) p, SUM(c) t\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE a > 1000 OR a < 5000\n"
                        + "GROUP BY a, b";

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
    public void testGroup7() throws SqlParseException {
        final String sql1 =
                "select a, nvl(b, null) as b1, count(distinct if(c > 0, b, a)) as c1 from test_db.test_table group by a, nvl(b, null)";
        final String sql2 =
                "select a, nvl(b, null) as b1, sum(d) as sd from test_db.test_table group by a, nvl(b, null)";
        final String expectSql =
                "SELECT a, nvl(b, NULL) b1, COUNT(DISTINCT if(c > 0, b, a)) c1, SUM(d) sd\n"
                        + "FROM test_db.test_table\n"
                        + "GROUP BY nvl(b, NULL), a";

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
    public void testGroup8() throws SqlParseException {

        // calcite: materialize with grouping sets not supported yet, so merge is meaningless
        final String sql1 =
                "select a, b, c, count(distinct if(c > 0, b, a)) as c1 from test_db.test_table group by  b, c grouping sets((),(a,b),(a,c))";
        final String sql2 =
                "select a, b, c, sum(d) as sd from test_db.test_table group by grouping sets((b,c),(a,b,c))";
        final String expectSql = "SELECT if(c > 0, b, a) $f3, a, b, c, d\nFROM test_db.test_table";

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
    public void testGroup82() throws SqlParseException {

        final String sql1 =
                "select a, b, c, count(distinct if(c > 0, b, a)) as c1 from test_db.test_table group by  b, c grouping sets((),(a,b),(a,c))";
        final String sql2 =
                "select a, b, c, sum(d) as sd from test_db.test_table group by grouping sets((b,c),(a,b,c))";
        final String expectSql =
                "SELECT a, b, c, COUNT(DISTINCT if(c > 0, b, a)) c1, SUM(d) sd\n"
                        + "FROM test_db.test_table\n"
                        + "GROUP BY GROUPING SETS((a, b, c), (b, c))";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);
        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1, false),
                        NodeUtils.createNodeRelRoot(relNode2, false));
        assertResultNode(expectSql, resultNode);

        resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode2, false),
                        NodeUtils.createNodeRelRoot(relNode1, false));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testGroupBy9() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(c) from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql =
                "SELECT a, b, c\nFROM test_db.test_table\nWHERE c < 5000 OR c < 1000";

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
    public void testGroupBy10() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) s_c from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, max(c) m_c from test_db.test_table WHERE c < 5000 group by a, b";
        final String expectSql =
                "SELECT a aa, b bb, a cc, b dd, MAX(c) m_c, SUM(c) s_c\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 5000\n"
                        + "GROUP BY a, b";

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
    public void testGroupBy11() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) s_c from test_db.test_table WHERE a < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, max(c) m_c from test_db.test_table group by a, b";
        final String expectSql =
                "SELECT a aa, b bb, a cc, b dd, MAX(c) m_c, SUM(c) s_c\n"
                        + "FROM test_db.test_table\n"
                        + "GROUP BY a, b";

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
    public void testGroupBy12() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) s_c from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, max(c) m_c from test_db.test_table group by a, b";
        final String expectSql = "SELECT a, b, c\nFROM test_db.test_table";

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
    }

    @Test
    public void testGroupBy13() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) s_c from test_db.test_table WHERE c < 5000 and a > 1000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, max(c) m_c from test_db.test_table WHERE a > 1000 and c < 5000 group by a, b";
        final String expectSql =
                "SELECT a aa, b bb, a cc, b dd, MAX(c) m_c, SUM(c) s_c\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE a > 1000 AND c < 5000\n"
                        + "GROUP BY a, b";

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
    public void testGroupBy14() throws SqlParseException {

        final String sql1 = "select a, sum(b), sum(c) from test_db.test_table  group by a ";
        final String sql2 = "select a, sum(c)  from test_db.test_table  group by a ";
        final String expectSql =
                "SELECT a, SUM(b), SUM(c), SUM(c)\nFROM test_db.test_table\n" + "GROUP BY a";

        final RelNode relNode1 = querySql2Rel(sql1);
        final RelNode relNode2 = querySql2Rel(sql2);
        ResultNode<RelNode> resultNode =
                NodeUtils.findFirstSubNode(
                        NodeUtils.createNodeRelRoot(relNode1),
                        NodeUtils.createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }
}
