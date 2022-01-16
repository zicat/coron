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

/** NodeRelFilterTest. */
public class NodeRelFilterTest extends NodeRelTest {

    public NodeRelFilterTest() {}

    @Test
    public void test2() throws SqlParseException {
        final String sql1 =
                "SELECT a as s, b AS aaa FROM test_db.test_table WHERE c < 5000 UNION ALL SELECT g, h FROM test_db.test_table2";
        final String sql2 = "SELECT a, b, c  FROM test_db.test_table WHERE c < 5000";
        final String expectSql =
                "SELECT a, b aaa, b, c, a s\nFROM test_db.test_table\nWHERE c < 5000";
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

    @Test
    public void testFilterProject() throws SqlParseException {

        final String sql1 =
                "SELECT a as s, b AS aaa FROM test_db.test_table WHERE c < 5000 UNION ALL SELECT g, h FROM test_db.test_table2";
        final String sql2 = "SELECT a, b, c  FROM test_db.test_table WHERE c < 1000";
        final String expectSql =
                "SELECT a, b aaa, b, c, a s\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 5000 OR c < 1000";

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
