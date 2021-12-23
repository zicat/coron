package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SubSqlTool;
import io.agora.cruise.analyzer.sql.SqlCsvIterable;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;

import java.util.*;

import static io.agora.cruise.analyzer.sql.SqlCsvIterator.CsvParser.FIRST_COLUMN;

/** SubSqlToolTest. */
public class SubSqlToolTest extends QueryTestBase {

    public static void main(String[] args) {
        SqlIterable sqlIterable = new SqlCsvIterable("query2.log", FIRST_COLUMN);

        QueryTestBase queryTestBase = new QueryTestBase();
        SubSqlTool subSqlTool = queryTestBase.createSubSqlTool(sqlIterable, sqlIterable, null);
        List<RelNode> viewQueryList = subSqlTool.start();

        Map<String, RelNode> viewNameQueryMapping = new HashMap<>();
        for (int i = 0; i < viewQueryList.size(); i++) {
            String viewName = "view_" + i;
            RelNode viewQuery = viewQueryList.get(i);
            queryTestBase.addMaterializedView(viewName, viewQuery);
            viewNameQueryMapping.put(viewName, viewQuery);
        }

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
                final Tuple2<Set<String>, RelNode> tuple2 =
                        queryTestBase.canMaterializedWithRelNode(
                                relNode, viewNameQueryMapping.keySet());
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
            System.out.println(queryTestBase.toSql(viewNameQueryMapping.get(viewName)));
            System.out.println("----------------------------------------------");
        }

        System.out.println(
                "total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
    }
}
