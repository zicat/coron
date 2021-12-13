package io.agora.cruise.presto.test;

import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import io.agora.cruise.presto.sql.SqlCsvIterable;
import io.agora.cruise.presto.sql.SqlIterator;
import io.agora.cruise.presto.sql.SqlTextIterable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static io.agora.cruise.presto.sql.SqlCsvIterator.CsvParser.FIRST_COLUMN;

/** ViewQueryAnalyzer. */
public class ViewQueryAnalyzer extends QueryTestBase {

    public static void main(String[] args) throws SqlParseException {

        QueryTestBase queryTestBase = new QueryTestBase();
        SqlIterator it =
                new SqlTextIterable("output/view_query.sql", StandardCharsets.UTF_8).sqlIterator();
        Set<String> viewNameSet = new HashSet<>();
        while (it.hasNext()) {
            String viewName = "view_" + it.currentOffset();
            queryTestBase.addMaterializedView(viewName, it.next());
            viewNameSet.add(viewName);
        }

        int total = 0;
        int matched = 0;
        Set<String> allMatchedView = new HashSet<>();

        SqlIterator iterator = new SqlCsvIterable("query2.log", FIRST_COLUMN).sqlIterator();
        while (iterator.hasNext()) {
            String querySql = iterator.next();
            try {
                if (!queryTestBase.sqlFilter.filter(querySql)) {
                    continue;
                }
                final RelNode relNode =
                        queryTestBase.querySql2Rel(querySql, new Int2BooleanConditionShuttle());
                final Tuple2<Set<String>, RelNode> tuple2 =
                        queryTestBase.canMaterializedWithRelNode(relNode, viewNameSet);
                if (!tuple2.f0.isEmpty()) {
                    matched++;
                    System.out.println("=====================================================");
                    System.out.println(querySql);
                    System.out.println("---------");
                    System.out.println(queryTestBase.toSql(tuple2.f1));
                    allMatchedView.addAll(tuple2.f0);
                }
                total++;
            } catch (Exception e) {
                queryTestBase.exceptionHandler.handle(null, null, e);
            }
        }

        System.out.println(
                "total:"
                        + total
                        + ",matched:"
                        + matched
                        + ",useless view:"
                        + (viewNameSet.size() - allMatchedView.size()));

        System.out.println("====useless view detail======");
        for (String view : viewNameSet) {
            if (!allMatchedView.contains(view)) {
                System.out.println(view);
                System.out.println("-----------------------------");
            }
        }
    }
}