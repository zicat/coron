package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.sql.SqlCsvIterable;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.agora.cruise.analyzer.sql.SqlCsvIterator.CsvParser.FIRST_COLUMN;

/** SubSqlToolTest. */
public class SubSqlToolTest extends QueryTestBase {

    public static void main(String[] args) {
        SqlIterable sqlIterable = new SqlCsvIterable("query2.log", FIRST_COLUMN);

        QueryTestBase queryTestBase = new QueryTestBase();
        SqlAnalyzer sqlAnalyzer = queryTestBase.createSqlAnalyzer();
        Map<String, RelNode> viewQueryMap = sqlAnalyzer.analyze(sqlIterable, sqlIterable);
        viewQueryMap.forEach(queryTestBase::addMaterializedView);

        int total = 0;
        int matched = 0;
        Set<String> allMatchedView = new HashSet<>();

        SqlIterator iterator = new SqlCsvIterable("query2.log", FIRST_COLUMN).sqlIterator();
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
        System.out.println("===========matched view================");
        for (String viewName : allMatchedView) {
            System.out.println(queryTestBase.toSql(viewQueryMap.get(viewName)));
            System.out.println("----------------------------------------------");
        }

        System.out.println(
                "total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
    }
}
