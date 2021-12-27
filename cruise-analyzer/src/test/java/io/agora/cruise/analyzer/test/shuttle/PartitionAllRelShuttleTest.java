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

/** PartitionAggregateFilterSimplifyTest. */
public class PartitionAllRelShuttleTest extends FileContext {

    public PartitionAllRelShuttleTest() {
        super("report_datahub");
    }

    @Test
    public void test0() throws SqlParseException {
        String sql =
                "select date,SUM(s2l_250ms_delay_sec) FROM pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        String expectSql =
                "SELECT date, SUM(s2l_250ms_delay_sec)\n"
                        + "FROM report_datahub.pub_levels_quality_di_1\n"
                        + "WHERE date_parse(CAST(date AS VARCHAR), '%Y%m%d') >= DATE('2021-11-21')\n"
                        + "GROUP BY date";
        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());

        List<String> partitionFields = Collections.singletonList("aaaa");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void test1() throws SqlParseException {
        String sql =
                "select date,SUM(s2l_250ms_delay_sec) FROM pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_280, SUM(s2l_250ms_delay_sec)\n"
                        + "FROM report_datahub.pub_levels_quality_di_1\n"
                        + "GROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')";
        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void test2() throws SqlParseException {
        String sql =
                "select date FROM report_datahub.pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        String expectSql =
                "SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_280\n"
                        + "FROM report_datahub.pub_levels_quality_di_1\n"
                        + "GROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')";
        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void test3() throws SqlParseException {
        String sql =
                "select date,sum(fiveSecJoinSuccess) from (select date,tag,sum(fiveSecJoinSuccess) as fiveSecJoinSuccess "
                        + "FROM report_datahub.pub_levels_quality_di_1 "
                        + "WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date, tag"
                        + ") as a where tag = '111' group by date";
        String expectSql =
                "SELECT date, tmp_p_280, SUM(fiveSecJoinSuccess)\n"
                        + "FROM (SELECT date, date_parse(CAST(date AS VARCHAR), '%Y%m%d') tmp_p_280, SUM(fivesecjoinsuccess) fiveSecJoinSuccess\n"
                        + "FROM report_datahub.pub_levels_quality_di_1\n"
                        + "WHERE tag = '111'\n"
                        + "GROUP BY date, date_parse(CAST(date AS VARCHAR), '%Y%m%d')) t2\n"
                        + "GROUP BY date, tmp_p_280";
        final RelNode relNode1 = querySql2Rel(sql, new Int2BooleanConditionShuttle());

        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        NodeRel nodeRel1 = createNodeRelRoot(shuttleChain.accept(relNode1));
        Assert.assertEquals(expectSql, toSql(nodeRel1.getPayload()));
    }

    @Test
    public void test4() throws SqlParseException {

        String sql =
                "select date,AVG(s2l_total_delay_sec) FROM report_datahub.pub_levels_quality_di_1 WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-21') group by date";
        final RelNode relNode = querySql2Rel(sql, new Int2BooleanConditionShuttle());
        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        Assert.assertNull(shuttleChain.accept(relNode));
    }

    @Test
    public void test5() throws SqlParseException {
        List<String> partitionFields = Collections.singletonList("date");
        RelShuttleChain shuttleChain =
                RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));
        Assert.assertEquals(expectSql5, toSql(shuttleChain.accept(querySql2Rel(sql5))));
    }

    private String expectSql5 =
            "SELECT t3.f1, t3.f2, t3.f3, t3.f4, t3.f5, t3.f6, t3.f7, t3.f8, t3.sum_totalUsage_ok, t3.f9, t3.f10, t3.level1, t14.__measure__0 sum_Calculation_4164281609521332225_ok, t3.sum_totalUsage_ok sum_totalUsage_ok0, t3.tmp_p_154, t14.tmp_p_154 tmp_p_1540, t14.tmp_p_1540 tmp_p_15400\n"
                    + "FROM (SELECT CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END level1, SUM(audience_sd) f1, SUM(host_hd) f2, SUM(video) f3, SUM(audience_hd) f4, SUM(audience_hdp) f5, SUM(host_hdp) f6, SUM(host_audio) f7, SUM(audio) f8, SUM(audience_audio) f9, SUM(host_sd) f10, SUM(total) sum_totalUsage_ok, date tmp_p_154\n"
                    + "FROM report_datahub.levels_usage_dod_di_1\n"
                    + "WHERE tag = 'Industry' AND dim = 1 AND level1 = 'Education' AND area IS NOT NULL\n"
                    + "GROUP BY CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END, date) t3\n"
                    + "INNER JOIN (SELECT t8.level1, t8.tmp_p_154, t12.tmp_p_154 tmp_p_1540, SUM(t12.__measure__1) __measure__0\n"
                    + "FROM (SELECT date, CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END level1, date tmp_p_154\n"
                    + "FROM report_datahub.levels_usage_dod_di_1\n"
                    + "WHERE tag = 'Industry' AND dim = 1 AND level1 = 'Education' AND area IS NOT NULL\n"
                    + "GROUP BY date, CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END, date) t8\n"
                    + "INNER JOIN (SELECT date, date tmp_p_154, SUM(total) __measure__1\n"
                    + "FROM report_datahub.levels_usage_dod_di_1\n"
                    + "WHERE tag = 'Industry' AND dim = 1 AND area IS NOT NULL AND level1 = 'Education' AND ('ALL' = 'ALL' OR area = 'ALL')\n"
                    + "GROUP BY date, date) t12 ON t8.date IS NOT DISTINCT FROM t12.date AND t8.tmp_p_154 = t12.tmp_p_154\n"
                    + "GROUP BY t8.level1, t8.tmp_p_154, t12.tmp_p_154) t14 ON t3.level1 IS NOT DISTINCT FROM t14.level1 AND t3.tmp_p_154 = t14.tmp_p_154";

    private String sql5 =
            "SELECT t1.f1, t1.f2, t1.f3, t1.f4, t1.f5, t1.f6, t1.f7, t1.f8, t1.sum_totalUsage_ok, t1.f9, t1.f10, t1.level1, t9.__measure__0 sum_Calculation_4164281609521332225_ok, t1.sum_totalUsage_ok\n"
                    + "FROM (SELECT CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END level1, SUM(audience_sd) f1, SUM(host_hd) f2, SUM(video) f3, SUM(audience_hd) f4, SUM(audience_hdp) f5, SUM(host_hdp) f6, SUM(host_audio) f7, SUM(audio) f8, SUM(audience_audio) f9, SUM(host_sd) f10, SUM(total) sum_totalUsage_ok\n"
                    + "FROM report_datahub.levels_usage_dod_di_1\n"
                    + "WHERE tag = 'Industry' AND (date >= 20210719 AND date <= 20210817) AND dim = 1 AND level1 = 'Education' AND area IS NOT NULL\n"
                    + "GROUP BY CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END) t1\n"
                    + "INNER JOIN (SELECT t4.level1, SUM(t7.__measure__1) __measure__0\n"
                    + "FROM (SELECT date, CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END level1\n"
                    + "FROM report_datahub.levels_usage_dod_di_1\n"
                    + "WHERE tag = 'Industry' AND (date >= 20210719 AND date <= 20210817) AND dim = 1 AND level1 = 'Education' AND area IS NOT NULL\n"
                    + "GROUP BY date, CASE WHEN level1 = 'unknown' THEN 'UNKNOWN' ELSE level1 END) t4\n"
                    + "INNER JOIN (SELECT date, SUM(total) __measure__1\n"
                    + "FROM report_datahub.levels_usage_dod_di_1\n"
                    + "WHERE tag = 'Industry' AND (date >= 20210719 AND date <= 20210817) AND (dim = 1 AND area IS NOT NULL AND (level1 = 'Education' AND ('ALL' = 'ALL' OR area = 'ALL')))\n"
                    + "GROUP BY date) t7 ON t4.date IS NOT DISTINCT FROM t7.date\n"
                    + "GROUP BY t4.level1) t9 ON t1.level1 IS NOT DISTINCT FROM t9.level1";
}
