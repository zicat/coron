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

package org.zicat.coron.analyzer.test.shuttle;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.coron.analyzer.rel.RelShuttleChain;
import org.zicat.coron.analyzer.shuttle.FilterRexNodeRollUpShuttle;
import org.zicat.coron.analyzer.test.QueryTestBase;
import org.zicat.coron.core.NodeRel;
import org.zicat.coron.parser.sql.shuttle.Int2BooleanConditionShuttle;

import static org.zicat.coron.core.NodeUtils.createNodeRelRoot;

/** PartitionAggregateFilterSimplifyTest. */
public class FilterRexNodeRollUpShuttleTest extends QueryTestBase {

    @Test
    public void testDiffPartition() throws SqlParseException {

        final String sql =
                "select date,SUM(m1) FROM test_db.test_table WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        final String expectSql =
                "SELECT date, SUM(m1)\nFROM test_db.test_table\nWHERE date_parse(CAST(date AS VARCHAR), '%Y%m%d') >= DATE('2021-11-21')\nGROUP BY date";

        final RelNode relNode1 = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("aaa"));
        final NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void testWithoutGroupSet() throws SqlParseException {
        final String sql =
                "select SUM(m1) FROM test_db.test_table WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21')";
        final String expectSql =
                "SELECT date_parse(CAST(date AS VARCHAR), '%Y%m%d') date_roll_up_5, SUM(m1)\nFROM test_db.test_table\nGROUP BY date_parse(CAST(date AS VARCHAR), '%Y%m%d')";

        final RelNode relNode1 = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        final NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void testFieldWithFunction() throws SqlParseException {

        final String sql =
                "select date,SUM(m1) FROM test_db.test_table WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        final String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') date_roll_up_5, SUM(m1)\n"
                        + "FROM test_db.test_table\nGROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')";

        final RelNode relNode1 = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        final NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void testViewOutAggregateFunction() throws SqlParseException {

        final String sql =
                "select date FROM test_db.test_table WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        final String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') date_roll_up_5\n"
                        + "FROM test_db.test_table\nGROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')";

        final RelNode relNode1 = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        final NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void testNestAggregation() throws SqlParseException {
        final String sql =
                "select date,sum(m1) from (select date,d1,sum(m1) as m1 "
                        + "FROM test_db.test_table "
                        + "WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date, d1"
                        + ") as a where d1 = '111' group by date";
        final String expectSql =
                "SELECT date, date_roll_up_5, SUM(m1)\n"
                        + "FROM (SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') date_roll_up_5, SUM(m1) m1\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE d1 = '111'\n"
                        + "GROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')) t2\n"
                        + "GROUP BY date, date_roll_up_5";

        final RelNode relNode1 = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        final NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void testFunctionWithoutRollUp() throws SqlParseException {

        final String sql =
                "select date,AVG(m1) FROM test_db.test_table WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";

        final RelNode relNode = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        Assert.assertNull(shuttleChain.accept(relNode));
    }

    @Test
    public void testWithFilter() throws SqlParseException {

        final String sql =
                "select date FROM test_db.test_table WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') ";
        final String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') date_roll_up_5\nFROM test_db.test_table";

        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        final NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void testWithFilterGroup() throws SqlParseException {
        final String sql =
                "SELECT d1, sum(m1) AS s1 FROM test_db.test_table WHERE d1 = 'aaa' and date in ('2022-01-01','2022-01-02') GROUP BY d1";
        final String expectSql =
                "SELECT d1, date date_roll_up_5, SUM(m1) s1\nFROM test_db.test_table\nWHERE d1 = 'aaa'\nGROUP BY d1, date";

        final RelNode relNode = querySql2Rel(sql);
        final RelShuttleChain shuttleChain =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
        final RelNode newRelNode = shuttleChain.accept(relNode);
        Assert.assertEquals(expectSql, toSql(newRelNode));

        final String sql2 =
                "SELECT d1, sum(m1) AS s1 FROM test_db.test_table WHERE date in ('2022-01-01','2022-01-02') GROUP BY d1";
        final String expectSql2 =
                "SELECT d1, date date_roll_up_5, SUM(m1) s1\nFROM test_db.test_table\nGROUP BY d1, date";
        final RelNode relNode2 = querySql2Rel(sql2);
        final RelNode newRelNode2 =
                RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"))
                        .accept(relNode2);
        Assert.assertEquals(expectSql2, toSql(newRelNode2));
    }
}
