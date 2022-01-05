package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlListIterable;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

/** SqlAnalyzerTest. */
public class SqlAnalyzerTest {

    @Test
    public void test() {
        final QueryTestBase queryTestBase = new QueryTestBase();
        final SqlAnalyzer sqlAnalyzer = queryTestBase.createSqlAnalyzer();
        final String sql1 =
                "select * from report_datahub.\"720p_brocaster_usage_1d\" where send_pixel = 'aaa'";
        final String sql2 =
                "select * from report_datahub.\"720p_brocaster_usage_1d\" where send_pixel = 'bbb'";
        final String expectSql =
                "SELECT _tidb_rowid, date, domain, send_pixel, video\n"
                        + "FROM report_datahub.720p_brocaster_usage_1d\n"
                        + "WHERE send_pixel = 'bbb' OR send_pixel = 'aaa'";
        final SqlIterable source = new SqlListIterable(Collections.singletonList(sql1));
        final SqlIterable target = new SqlListIterable(Collections.singletonList(sql2));
        final Map<String, RelNode> result = sqlAnalyzer.start(source, target);
        Assert.assertEquals(1, result.size());
        final RelNode relNode = result.get(sqlAnalyzer.viewName(0));
        final String viewQuery = queryTestBase.toSql(relNode);
        Assert.assertEquals(expectSql, viewQuery);
    }
}
