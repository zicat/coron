package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** SubSqlToolByQuery2Test. */
public class SubSqlToolByQuery11Test {

    public static void main(String[] args) {
        test();
    }

    public static void test() {

        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
        SqlIterable source = new SqlJsonIterable("/Users/zj/Desktop/query_20.json", parser);
        SqlIterable target = new SqlJsonIterable("/Users/zj/Desktop/query_21.json", parser);
        QueryTestBase queryTestBase = new QueryTestBase();
        SqlAnalyzer sqlAnalyzer = queryTestBase.createSubSqlTool(source, target);
        Map<String, RelNode> viewQueryMap = sqlAnalyzer.start();
        viewQueryMap.forEach(queryTestBase::addMaterializedView);

        int total = 0;
        int matched = 0;
        Set<String> allMatchedView = new HashSet<>();

        SqlIterator iterator =
                new SqlJsonIterable("/Users/zj/Desktop/query_22.json", parser).sqlIterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.currentOffset());
            String querySql = iterator.next();
            try {
                if (queryTestBase.sqlFilter.filter(querySql)) {
                    continue;
                }
                final RelNode relNode =
                        queryTestBase.querySql2Rel(querySql, new Int2BooleanConditionShuttle());
                final Tuple2<Set<String>, RelNode> tuple2 =
                        queryTestBase.canMaterializedWithRelNode(relNode, viewQueryMap.keySet());
                if (!tuple2.f0.isEmpty()) {
                    matched++;
                    allMatchedView.addAll(tuple2.f0);
                }
                total++;
            } catch (Exception e) {
                queryTestBase.exceptionHandler.handle(querySql, e);
            }
        }

        System.out.println("===========matched view================");
        for (String viewName : allMatchedView) {
            System.out.println(queryTestBase.toSql(viewQueryMap.get(viewName)));
            System.out.println("----------------------------------------------");
        }
        System.out.println(
                "total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
    }
}
