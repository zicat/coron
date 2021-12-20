package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.analyzer.sql.SqlTextIterable;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** ViewQueryAnalyzer1113Test. */
public class ViewQueryAnalyzer1113Test {

    public static void main(String[] args) throws SqlParseException {

        QueryTestBase queryTestBase = new QueryTestBase();
        SqlIterator it =
                new SqlTextIterable("output/view_query_2.sql", StandardCharsets.UTF_8)
                        .sqlIterator();
        Map<String, String> viewNameQueryMapping = new HashMap<>();
        while (it.hasNext()) {
            String viewName = "view_" + it.currentOffset();
            String viewQuery = it.next();
            queryTestBase.addMaterializedView(viewName, viewQuery);
            viewNameQueryMapping.put(viewName, viewQuery);
        }

        int total = 0;
        int matched = 0;
        Set<String> allMatchedView = new HashSet<>();

        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
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

        System.out.println("===========matched view================");
        for (String viewName : allMatchedView) {
            System.out.println(viewNameQueryMapping.get(viewName));
            System.out.println("----------------------------------------------");
        }

        System.out.println(
                "total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
    }
}
