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

package org.zicat.coron.analyzer.test;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.coron.analyzer.SqlAnalyzer;
import org.zicat.coron.analyzer.sql.SqlIterable;
import org.zicat.coron.analyzer.sql.SqlListIterable;

import java.util.Collections;
import java.util.Map;

/** SqlAnalyzerTest. */
public class SqlAnalyzerTest {

    @Test
    public void test() {
        final QueryTestBase queryTestBase = new QueryTestBase();
        final SqlAnalyzer sqlAnalyzer = queryTestBase.createSqlAnalyzer();
        final String sql1 = "select * from test_db.test_table where d1 = 'aaa'";
        final String sql2 = "select * from test_db.test_table where d1 = 'bbb'";
        final String expectSql =
                "SELECT d1, d2, date, m1, m2\nFROM test_db.test_table\nWHERE d1 = 'bbb' OR d1 = 'aaa'";
        final SqlIterable source = new SqlListIterable(Collections.singletonList(sql1));
        final SqlIterable target = new SqlListIterable(Collections.singletonList(sql2));
        final Map<String, RelNode> result = sqlAnalyzer.analyze(source, target);
        Assert.assertEquals(1, result.size());
        final RelNode relNode = result.get(sqlAnalyzer.viewName(0));
        final String viewQuery = queryTestBase.toSql(relNode);
        Assert.assertEquals(expectSql, viewQuery);
    }
}
