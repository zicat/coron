package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SubSqlTool;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/** SubSqlToolByQuery2Test. */
public class SubSqlToolByQuery10Test {

    @Test
    public void test() throws SqlParseException {

        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
        SqlIterable source = new SqlJsonIterable("query_11.json", parser);
        SqlIterable target = new SqlJsonIterable("query_10.json", parser);
        QueryTestBase queryTestBase = new QueryTestBase();
        SubSqlTool subSqlTool =
                queryTestBase.createSubSqlTool(source, target, sql -> !sql.contains("WHERE"));
        Set<String> viewQuerySet = subSqlTool.start();
        List<String> viewQueryList =
                viewQuerySet.stream()
                        .map(v -> v.replace("\n", " ").replace("\r", " "))
                        .collect(Collectors.toList());

        Map<String, String> viewNameQueryMapping = new HashMap<>();
        for (int i = 0; i < viewQueryList.size(); i++) {
            final String viewName = "view_" + i;
            final String viewQuery = viewQueryList.get(i);
            queryTestBase.addMaterializedView(viewName, viewQuery);
            viewNameQueryMapping.put(viewName, viewQuery);
        }

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
                final Tuple2<Set<String>, RelNode> tuple2 =
                        queryTestBase.canMaterializedWithRelNode(
                                relNode, viewNameQueryMapping.keySet());
                if (!tuple2.f0.isEmpty()) {
                    matched++;
                    allMatchedView.addAll(tuple2.f0);
                }
                total++;
            } catch (Exception e) {
                queryTestBase.exceptionHandler.handle(null, null, e);
            }
        }

        String expectView =
                "SELECT * FROM \"levels_usage_dod_di_1\" WHERE \"tag\" = 'Product' AND \"dim\" = 1 OR \"tag\" = 'Industry' AND \"dim\" = 1 AND ('ALL' = 'ALL' OR \"area\" = 'ALL')";
        Assert.assertEquals(313, total);
        Assert.assertEquals(176, matched);
        Optional<String> viewName = allMatchedView.stream().findFirst();
        Assert.assertTrue(viewName.isPresent());
        Assert.assertEquals(expectView, viewNameQueryMapping.get(viewName.get()));
    }
}
