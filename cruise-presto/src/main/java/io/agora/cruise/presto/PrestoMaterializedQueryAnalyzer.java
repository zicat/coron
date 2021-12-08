package io.agora.cruise.presto;

import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** PrestoMaterializedQueryAnalyzer. */
public class PrestoMaterializedQueryAnalyzer extends PrestoQueryTest {

    public static void main(String[] args) throws IOException, SqlParseException {
        CalciteContext context = context();
        List<String> viewQueryList = getViewQuery();
        Set<String> viewNameSet = new HashSet<>();
        for (int i = 0; i < viewQueryList.size(); i++) {
            String viewQuery = viewQueryList.get(i);
            String viewName = "view_" + i;
            context.addMaterializedView(viewName, viewQuery);
            viewNameSet.add(viewName);
        }
        List<String> querySqlList = querySqlList();
        int total = 0;
        int matched = 0;
        for (String querySql : querySqlList) {
            try {
                final SqlNode sqlNode =
                        SqlNodeTool.toQuerySqlNode(querySql, new Int2BooleanConditionShuttle());
                final RelNode relNode =
                        context.createSqlToRelConverter().convertQuery(sqlNode, true, true).rel;
                if (canMaterialized(relNode, viewNameSet, context)) {
                    matched++;
                    System.out.println("=====================================================");
                    System.out.println(querySql);
                    System.out.println("---------");
                    System.out.println(context.toSql(context.materializedViewOpt(relNode)));
                }
                total++;
            } catch (Exception e) {
                if (e.toString().contains("Object 'media' not found")
                        || e.toString().contains("Object 'queries' not found")
                        || e.toString().contains("Object 'information_schema' not found")) {
                    continue;
                }
                throw e;
            }
        }
        System.out.println("total:" + total + ",matched:" + matched);
    }

    private static List<String> getViewQuery() throws IOException {
        BufferedReader br =
                new BufferedReader(
                        new InputStreamReader(
                                Objects.requireNonNull(
                                        Thread.currentThread()
                                                .getContextClassLoader()
                                                .getResourceAsStream("public-sql.txt")),
                                StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String line;
        List<String> viewQuery = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            if (line.startsWith("===========")) {
                String sql = sb.toString();
                if (!sql.isEmpty()) {
                    viewQuery.add(sql);
                }
                sb = new StringBuilder();
            } else {
                sb.append(line);
                sb.append(System.lineSeparator());
            }
        }
        String sql = sb.toString();
        if (!sql.isEmpty()) {
            viewQuery.add(sql);
        }
        return viewQuery;
    }
}
