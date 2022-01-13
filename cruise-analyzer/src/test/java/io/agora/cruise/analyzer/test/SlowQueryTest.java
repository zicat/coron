package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.analyzer.sql.SqlTextIterable;
import io.agora.cruise.analyzer.sql.dialect.PrestoDialect;
import io.agora.cruise.parser.SqlNodeUtils;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.util.Pair;
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
        String viewQuery = it.next();
        String viewName = queryTestBase.defaultDatabase() + ".view_query_0";
        SqlNode sqlNode =
                SqlNodeUtils.toSqlNode(viewQuery, SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG);
        RelNode viewQueryRoot = queryTestBase.sqlNode2RelNode(sqlNode);
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
                        queryTestBase.querySql2Rel(querySql, new Int2BooleanConditionShuttle());
                Pair<Set<String>, RelNode> tuple2 = queryTestBase.tryMaterialized(relNode);
                if (!tuple2.left.isEmpty()) {
                    matched++;
                    allMatchedView.addAll(tuple2.left);
                    newQueryMap.put(
                            querySql, queryTestBase.toSql(tuple2.right, PrestoDialect.DEFAULT));
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
        Map<String, Pair<Long, Long>> result = new HashMap<>();
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
            Pair<Long, Long> tuple2 = Pair.of(oldSpendMin, newSpendMin);
            result.put(oldQuery, tuple2);
            System.out.println(tuple2);
        }
        long oldTotal =
                result.values().stream().mapToLong(longLongTuple2 -> longLongTuple2.left).sum();
        long newTotal =
                result.values().stream().mapToLong(longLongTuple2 -> longLongTuple2.right).sum();
        System.out.println("total spend " + Pair.of(oldTotal, newTotal));
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
