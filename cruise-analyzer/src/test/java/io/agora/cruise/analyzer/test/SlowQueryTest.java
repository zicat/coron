package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.analyzer.sql.SqlTextIterable;
import io.agora.cruise.analyzer.sql.dialect.PrestoDialect;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.shuttle.HavingCountShuttle;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/** SlowQueryTest. */
public class SlowQueryTest {

    private static final Logger LOG = LoggerFactory.getLogger(SlowQueryTest.class);

    public static void main(String[] args) throws SqlParseException, SQLException {
        QueryTestBase queryTestBase = new QueryTestBase();
        SqlTextIterable sqlTextIterable =
                new SqlTextIterable("view_query.txt", StandardCharsets.UTF_8);
        SqlIterator it = sqlTextIterable.sqlIterator();
        Map<String, String> allViews = new HashMap<>();
        String viewQuery = it.next();
        String viewName = queryTestBase.defaultDatabase() + ".view_query_0";
        SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(viewQuery);
        RelNode viewQueryRoot = queryTestBase.sqlNode2RelNode(sqlNode);
        allViews.put(viewName, viewQuery);
        queryTestBase.addMaterializedView(viewName, viewQueryRoot);

        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
        SqlIterator iterator = (new SqlJsonIterable("slow_query.json", parser)).sqlIterator();
        int matched = 0;
        int total = 0;
        Set<String> allMatchedView = new HashSet<>();
        Map<String, String> newQueryMap = new HashMap<>();
        while (iterator.hasNext()) {
            String querySql = iterator.next();
            try {
                if (queryTestBase.sqlFilter.filter(querySql)) {
                    continue;
                }
                RelNode relNode =
                        queryTestBase.querySql2Rel(
                                querySql,
                                new Int2BooleanConditionShuttle(),
                                new HavingCountShuttle());
                Tuple2<Set<String>, RelNode> tuple2 =
                        queryTestBase.canMaterializedWithRelNode(relNode, allViews.keySet());
                if (!tuple2.f0.isEmpty()) {
                    matched++;
                    allMatchedView.addAll(tuple2.f0);
                    newQueryMap.put(
                            querySql, queryTestBase.toSql(tuple2.f1, PrestoDialect.DEFAULT));
                }
                total++;
            } catch (Exception e) {
                queryTestBase.exceptionHandler.handle(querySql, e);
            }
        }
        System.out.println(
                "==========================================================================");
        newQueryMap.forEach(
                (k, v) -> {
                    System.out.println("----------------替换前---------------");
                    System.out.println(k);
                    System.out.println("----------------替换后---------------");
                    System.out.println(v);
                });
        LOG.info("total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
        Map<String, Tuple2<Long, Long>> result = new HashMap<>();
        int tryCount = 1;
        for (Map.Entry<String, String> entry : newQueryMap.entrySet()) {
            String oldQuery = entry.getKey();
            String newQuery = entry.getValue();
            long oldSpendMin = Long.MAX_VALUE;
            for (int i = 0; i < tryCount; i++) {
                oldSpendMin = Math.min(oldSpendMin, processByPrestoSql(oldQuery));
            }
            long newSpendMin = Long.MAX_VALUE;
            for (int i = 0; i < tryCount; i++) {
                newSpendMin = Math.min(newSpendMin, processByPrestoSql(newQuery));
            }
            Tuple2<Long, Long> tuple2 = Tuple2.of(oldSpendMin, newSpendMin);
            result.put(oldQuery, tuple2);
            System.out.println(tuple2);
        }
        long oldTotal =
                result.values().stream().mapToLong(longLongTuple2 -> longLongTuple2.f0).sum();
        long newTotal =
                result.values().stream().mapToLong(longLongTuple2 -> longLongTuple2.f1).sum();
        System.out.println("total spend " + Tuple2.of(oldTotal, newTotal));
    }

    public static long processByPrestoSql(String inputSql) throws SQLException {
        long start = System.currentTimeMillis();
        String url = "jdbc:trino://10.11.13.30:8080/hive/report_datahub";
        Properties properties = new Properties();
        properties.setProperty("user", "data_report");
        try (Connection conn = DriverManager.getConnection(url, properties);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(inputSql)) {
            while (rs.next()) {}
        }
        return System.currentTimeMillis() - start;
    }
}
