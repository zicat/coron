package io.agora.cruise.core.test.presto;

import io.agora.cruise.core.test.NodeRelTest;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** PrestoMaterializedQueryAnalyzer. */
public class PrestoMaterializedQueryAnalyzer extends PrestoQueryTest {

    public static void main(String[] args) throws IOException, SqlParseException {
        List<String> viewQueryList = getViewQuery();
        Set<String> viewNameSet = new HashSet<>();
        for (int i = 0; i < viewQueryList.size(); i++) {
            String viewQuery = viewQueryList.get(i);
            String viewName = NODE_REL_TEST.dynamicViewName() + "_" + i;
            NODE_REL_TEST.addMaterializedView(viewName, viewQuery);
            viewNameSet.add(viewName);
        }
        List<String> querySqlList = querySqlList();
        int total = querySqlList.size();
        int matched = 0;
        for (String querySql : querySqlList) {
            try {
                final SqlNode sqlNode =
                        SqlNodeTool.toQuerySqlNode(querySql, new Int2BooleanConditionShuttle());
                final RelNode relNode =
                        NODE_REL_TEST
                                .createSqlToRelConverter()
                                .convertQuery(sqlNode, true, true)
                                .rel;
                if (canMaterialized(relNode, viewNameSet)) {
                    matched++;
                    System.out.println("=====================================================");
                    System.out.println(querySql);
                    System.out.println("---------");
                    System.out.println(
                            NODE_REL_TEST.toSql(NODE_REL_TEST.materializedViewOpt(relNode)));
                }
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
                                                .getResourceAsStream(
                                                        "presto_query/public-sql.txt")),
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

    private static final NodeRelTest nodeRelTest;

    static {
        try {
            nodeRelTest = new NodeRelTest("report_datahub");
            LineIterator it =
                    IOUtils.lineIterator(
                            Objects.requireNonNull(
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResourceAsStream("presto_query/ddl.txt")),
                            StandardCharsets.UTF_8);
            while (it.hasNext()) {
                String ddl = it.next();
                String[] split = ddl.split("\\s+");
                String table = split[2];
                String[] tableSplit = table.split("\\.");
                String newTable;
                if (tableSplit.length < 2 || tableSplit.length > 3) {
                    continue;
                } else if (tableSplit.length == 2) {
                    newTable = tableSplit[0] + ".\"" + tableSplit[1] + "\"";
                } else {
                    newTable = tableSplit[1] + ".\"" + tableSplit[2] + "\"";
                }
                ddl = ddl.replace(table, newTable);
                nodeRelTest.addTables(ddl);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
