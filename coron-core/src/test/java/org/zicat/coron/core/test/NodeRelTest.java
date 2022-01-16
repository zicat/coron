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
import org.apache.calcite.sql.dialect.DefaultSqlDialect;
import org.junit.Assert;
import org.zicat.coron.core.ResultNode;

import java.util.Set;

/** NodeRelTest. */
public class NodeRelTest extends TestBase {

    public NodeRelTest() {}

    public void assertResultNode(String expectSql, ResultNode<RelNode> resultNode) {
        Assert.assertEquals(expectSql, toSql(resultNode));
    }

    public String toSql(ResultNode<RelNode> resultNode) {
        return relNode2SqlNode(resultNode.getPayload())
                .toSqlString(DefaultSqlDialect.DEFAULT)
                .getSql();
    }

    public void assertMaterialized(
            String materializeViewName, ResultNode<RelNode> resultNode, RelNode node2Opt) {
        addMaterializedView(materializeViewName, resultNode.getPayload());
        final RelNode optRelNode1 = materializedViewOpt(node2Opt).left;
        final Set<String> optTableNames = tables(optRelNode1);
        final String fullMaterializeViewName = defaultDatabase() + "." + materializeViewName;
        Assert.assertTrue(optTableNames.contains(fullMaterializeViewName));
    }

    public String dynamicViewName() {
        Throwable t = new Throwable();
        StackTraceElement[] st = t.getStackTrace();
        return "view_" + st[1].getMethodName();
    }
}
