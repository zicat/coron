package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** SubSqlToolByQuery2Test. */
public class SqlAnalyzerByQuery10Test {

    private static final Logger LOG = LoggerFactory.getLogger(SqlAnalyzerByQuery10Test.class);

    @Test
    public void test() {

        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
        SqlIterable source = new SqlJsonIterable("query_11.json", parser);
        SqlIterable target = new SqlJsonIterable("query_10.json", parser);
        QueryTestBase queryTestBase = new QueryTestBase();
        SqlAnalyzer sqlAnalyzer = queryTestBase.createSqlAnalyzer();
        Map<String, RelNode> viewQueryMap = sqlAnalyzer.analyze(source, target);
        viewQueryMap.forEach(queryTestBase::addMaterializedView);

        int total = 0;
        int matched = 0;
        Set<String> allMatchedView = new HashSet<>();

        SqlIterator iterator = new SqlJsonIterable("query_13.json", parser).sqlIterator();
        while (iterator.hasNext()) {
            String querySql = iterator.next();
            try {
                if (queryTestBase.sqlFilter.filter(querySql)) {
                    continue;
                }
                final RelNode relNode =
                        queryTestBase.querySql2Rel(querySql, new Int2BooleanConditionShuttle());
                final Pair<Set<String>, RelNode> tuple2 = queryTestBase.tryMaterialized(relNode);
                if (!tuple2.left.isEmpty()) {
                    matched++;
                    allMatchedView.addAll(tuple2.left);
                }
                total++;
            } catch (Exception e) {
                queryTestBase.exceptionHandler.handle(querySql, e);
            }
        }

        LOG.info("===========matched view================");
        for (String viewName : allMatchedView) {
            LOG.info(queryTestBase.toSql(viewQueryMap.get(viewName)));
            LOG.info("----------------------------------------------");
        }
        Assert.assertEquals(313, total);
        Assert.assertFalse(allMatchedView.isEmpty());
        LOG.info("total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
    }
}
