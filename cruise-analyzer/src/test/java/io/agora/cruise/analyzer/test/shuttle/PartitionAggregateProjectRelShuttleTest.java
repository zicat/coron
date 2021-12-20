package io.agora.cruise.analyzer.test.shuttle;

import io.agora.cruise.analyzer.FileContext;
import io.agora.cruise.analyzer.shuttle.PartitionRelShuttle;
import io.agora.cruise.core.NodeRel;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;

/** PartitionAggregateFilterSimplifyTest. */
public class PartitionAggregateProjectRelShuttleTest {

    @Test
    public void test2() throws SqlParseException {
        FileContext context = new FileContext("report_datahub");
        String sql =
                "select date FROM report_datahub.pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_1\n"
                        + "FROM report_datahub.pub_levels_quality_di_1\n"
                        + "GROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')";
        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql, new Int2BooleanConditionShuttle());
        final RelNode relNode1 = context.sqlNode2RelNode(sqlNode1);

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(relNode1, shuttleChain);
        Assert.assertEquals(expectSql, context.toSql(nodeRel1.getPayload()));
    }

    @Test
    public void test3() throws SqlParseException {
        FileContext context = new FileContext("report_datahub");
        String sql =
                "select date,sum(fiveSecJoinSuccess) from (select date,tag,sum(fiveSecJoinSuccess) as fiveSecJoinSuccess "
                        + "FROM report_datahub.pub_levels_quality_di_1 "
                        + "WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date, tag"
                        + ") as a where tag = '111' group by date";
        String expectSql =
                "SELECT date, SUM(fiveSecJoinSuccess), tmp_p_2\n"
                        + "FROM (SELECT date, SUM(fivesecjoinsuccess) fiveSecJoinSuccess, date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_2\n"
                        + "FROM report_datahub.pub_levels_quality_di_1\n"
                        + "WHERE tag = '111'\n"
                        + "GROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')) t2\n"
                        + "GROUP BY date, tmp_p_2";
        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql, new Int2BooleanConditionShuttle());
        final RelNode relNode1 = context.sqlNode2RelNode(sqlNode1);

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(relNode1, shuttleChain);
        Assert.assertEquals(expectSql, context.toSql(nodeRel1.getPayload()));
    }
}
