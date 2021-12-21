package io.agora.cruise.analyzer.test.shuttle;

import io.agora.cruise.analyzer.FileContext;
import io.agora.cruise.analyzer.shuttle.PartitionRelShuttle;
import io.agora.cruise.core.NodeRel;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;

/** PartitionFilterSimplifyTest. */
public class PartitionProjectFilterRelShuttleTest extends FileContext {

    public PartitionProjectFilterRelShuttleTest() {
        super("report_datahub");
    }

    @Test
    public void test() throws SqlParseException {
        String sql =
                "select date FROM report_datahub.pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') ";
        String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_1\n"
                        + "FROM report_datahub.pub_levels_quality_di_1";

        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(relNode1, shuttleChain);
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void test2() throws SqlParseException {
        String sql =
                "select date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') FROM report_datahub.pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21')";
        String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d'), date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_2\n"
                        + "FROM report_datahub.pub_levels_quality_di_1";

        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(relNode1, shuttleChain);
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }
}
