package io.agora.cruise.presto;

import com.csvreader.CsvReader;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PrestoDialect;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findAllSubNode;

/** PrestoQueryTest. */
public class PrestoQueryTest {

    public static void main(String[] args) throws Exception {
        CalciteContext calciteContext = context();
        List<String> querySql = querySqlList();
        BufferedWriter bw =
                createWriter(System.getProperty("user.home") + "/Desktop/public-sql.txt");
        BufferedWriter bw2 =
                createWriter(System.getProperty("user.home") + "/Desktop/source-sql.txt");
        Map<String, Integer> result = new HashMap<>();

        HepPlanner planner =
                context()
                        .createPlanner(
                                Collections.singletonList(
                                        AggregateProjectMergeRule.Config.DEFAULT.toRule()));
        for (int i = 0; i < querySql.size() - 1; i++) {
            for (int j = i + 1; j < querySql.size(); j++) {
                String fromSql = querySql.get(i);
                String toSql = querySql.get(j);
                try {
                    final SqlNode sqlNode1 =
                            SqlNodeTool.toQuerySqlNode(fromSql, new Int2BooleanConditionShuttle());
                    final SqlNode sqlNode2 =
                            SqlNodeTool.toQuerySqlNode(toSql, new Int2BooleanConditionShuttle());
                    final RelNode relNode1 =
                            calciteContext
                                    .createSqlToRelConverter()
                                    .convertQuery(sqlNode1, true, true)
                                    .rel;
                    final RelNode relNode2 =
                            calciteContext
                                    .createSqlToRelConverter()
                                    .convertQuery(sqlNode2, true, true)
                                    .rel;
                    ResultNodeList<RelNode> resultNodeList =
                            findAllSubNode(
                                    createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
                    if (resultNodeList.isEmpty()) {
                        continue;
                    }

                    List<Integer> ids = new ArrayList<>();
                    Set<String> viewNames = new HashSet<>();
                    for (ResultNode<RelNode> resultNode : resultNodeList) {

                        if (!calciteContext
                                .toSql(resultNode.getPayload(), PrestoDialect.DEFAULT)
                                .contains("GROUP BY")) {
                            continue;
                        }

                        planner.setRoot(resultNode.getPayload());
                        String resultSql =
                                calciteContext.toSql(planner.findBestExp(), PrestoDialect.DEFAULT);
                        Integer value = result.get(resultSql);
                        if (value == null) {
                            value = result.size();
                            result.put(resultSql, value);
                            bw.write(
                                    "======================similar sql "
                                            + value
                                            + "==================");
                            bw.newLine();
                            bw.write(
                                    calciteContext.toSql(
                                            resultNode.getPayload(), PrestoDialect.DEFAULT));
                            bw.newLine();
                            bw.flush();
                            calciteContext.addMaterializedView(
                                    "view_" + value, resultNode.getPayload());
                        }
                        viewNames.add("view_" + value);
                        ids.add(value);
                    }

                    String value =
                            ids.stream()
                                    .sorted()
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(","));

                    if (!viewNames.isEmpty()
                            && !canMaterialized(relNode1, viewNames, calciteContext)) {
                        System.out.println(
                                "===============not match:"
                                        + value
                                        + "============================");
                        System.out.println(fromSql);
                    }
                    if (!viewNames.isEmpty()
                            && !canMaterialized(relNode2, viewNames, calciteContext)) {
                        System.out.println(
                                "===============not match:"
                                        + value
                                        + "============================");
                        System.out.println(toSql);
                    }

                    bw2.write(
                            "=========================from sql mapping to ="
                                    + value
                                    + "========================");
                    bw2.newLine();
                    bw2.write(fromSql);
                    bw2.newLine();
                    bw2.write(
                            "=========================to sql mapping to ="
                                    + value
                                    + "========================");
                    bw2.newLine();
                    bw2.write(toSql);
                    bw2.newLine();
                    bw2.flush();

                } catch (Throwable e) {
                    if (e.toString().contains("Object 'media' not found")
                            || e.toString().contains("Object 'queries' not found")
                            || e.toString().contains("Object 'information_schema' not found")) {
                        continue;
                    }
                    System.out.println("======================");
                    System.out.println(fromSql);
                    System.out.println("======================");
                    System.out.println(toSql);
                    throw e;
                }
            }
        }
        bw.close();
        bw2.close();
    }

    public static boolean canMaterialized(
            RelNode relNode, Set<String> viewNames, CalciteContext calciteContext) {
        RelNode optRelNode1 = calciteContext.materializedViewOpt(relNode);
        Set<String> opRelNode1Tables = TableRelShuttleImpl.tables(optRelNode1);

        for (String opRelNode1Table : opRelNode1Tables) {
            if (viewNames.contains(opRelNode1Table)) {
                return true;
            }
        }
        return false;
    }

    public static List<String> querySqlList() throws IOException {
        Set<String> distinctSql = new HashSet<>();
        csvReaderHandler(csvReader -> distinctSql.add(csvReader.get(0)));
        List<String> querySql = new ArrayList<>(distinctSql);
        querySql =
                querySql.stream()
                        .filter(v -> !v.startsWith("explain"))
                        .filter(v -> !v.contains("system.runtime"))
                        .filter(v -> !v.toUpperCase().startsWith("SHOW "))
                        .collect(Collectors.toList());
        return querySql;
    }

    private static BufferedWriter createWriter(String fileName) throws IOException {
        File file = new File(fileName);
        if (file.exists() && !file.delete()) {
            throw new IOException("delete fail" + fileName);
        }
        if (!file.createNewFile()) {
            throw new IOException("create fail " + fileName);
        }
        return new BufferedWriter(new FileWriter(file));
    }

    /** Handler. */
    public interface Handler {

        void handle(CsvReader csvReader) throws IOException;
    }

    public static void csvReaderHandle(Reader reader, Handler handler) throws IOException {
        CsvReader csvReader = new CsvReader(reader);
        csvReader.readHeaders();
        while (csvReader.readRecord()) {
            handler.handle(csvReader);
        }
    }

    public static void csvReaderHandler(Handler handler) throws IOException {
        Reader reader =
                new InputStreamReader(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("query2.log")),
                        StandardCharsets.UTF_8);
        csvReaderHandle(reader, handler);
    }

    public static CalciteContext context() {
        CalciteContext calciteContext = new CalciteContext("report_datahub");
        try {
            LineIterator it =
                    IOUtils.lineIterator(
                            Objects.requireNonNull(
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResourceAsStream("ddl.txt")),
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
                calciteContext.addTables(ddl);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return calciteContext;
    }
}
