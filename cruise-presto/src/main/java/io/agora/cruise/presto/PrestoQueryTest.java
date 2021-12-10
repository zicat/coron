package io.agora.cruise.presto;

import io.agora.cruise.core.NodeRel;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import io.agora.cruise.presto.simplify.PartitionAggregateFilterSimplify;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PrestoDialect;

import java.io.BufferedWriter;
import java.util.*;
import java.util.stream.Collectors;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findAllSubNode;
import static io.agora.cruise.presto.PrestoContext.createWriter;
import static io.agora.cruise.presto.PrestoContext.querySqlList;

/** PrestoQueryTest. */
public class PrestoQueryTest {

    public static void main(String[] args) throws Exception {
        PrestoContext context = new PrestoContext();
        List<String> querySql = querySqlList();
        BufferedWriter bw = createWriter("output/public-sql.txt");
        Map<String, Integer> result = new HashMap<>();

        NodeRel.Simplify simplify =
                new PartitionAggregateFilterSimplify(Collections.singletonList("date"));
        for (int i = 0; i < querySql.size() - 1; i++) {
            for (int j = i + 1; j < querySql.size(); j++) {
                String fromSql = querySql.get(i);
                String toSql = querySql.get(j);
                try {
                    final SqlNode sqlNode1 =
                            SqlNodeTool.toQuerySqlNode(fromSql, new Int2BooleanConditionShuttle());
                    final SqlNode sqlNode2 =
                            SqlNodeTool.toQuerySqlNode(toSql, new Int2BooleanConditionShuttle());
                    final RelNode relNode1 = context.sqlNode2RelNode(sqlNode1);
                    final RelNode relNode2 = context.sqlNode2RelNode(sqlNode2);
                    ResultNodeList<RelNode> resultNodeList =
                            findAllSubNode(
                                    createNodeRelRoot(relNode1, simplify),
                                    createNodeRelRoot(relNode2, simplify));
                    if (resultNodeList.isEmpty()) {
                        continue;
                    }

                    List<Integer> ids = new ArrayList<>();
                    Set<String> viewNames = new HashSet<>();
                    for (ResultNode<RelNode> resultNode : resultNodeList) {

                        String resultSql =
                                context.toSql(resultNode.getPayload(), PrestoDialect.DEFAULT);
                        if (!resultSql.contains("GROUP BY") && !resultSql.contains("JOIN")) {
                            continue;
                        }
                        if (!resultSql.contains("WHERE")) {
                            continue;
                        }

                        Integer value = result.get(resultSql);
                        if (value == null) {
                            value = result.size();
                            result.put(resultSql, value);
                            bw.write(
                                    "======================similar sql "
                                            + value
                                            + "==================");
                            bw.newLine();
                            bw.write(resultSql);
                            bw.newLine();
                            bw.flush();
                            context.addMaterializedView("view_" + value, resultNode.getPayload());
                        }
                        viewNames.add("view_" + value);
                        ids.add(value);
                    }
                    if (ids.isEmpty()) {
                        continue;
                    }

                    String value =
                            ids.stream()
                                    .sorted()
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(","));

                    if (!viewNames.isEmpty()
                            && context.canMaterialized(relNode1, viewNames).isEmpty()) {
                        System.out.println(
                                "===============not match:"
                                        + value
                                        + "============================");
                        System.out.println(fromSql);
                    }
                    if (!viewNames.isEmpty()
                            && context.canMaterialized(relNode2, viewNames).isEmpty()) {
                        System.out.println(
                                "===============not match:"
                                        + value
                                        + "============================");
                        System.out.println(toSql);
                    }

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
    }
}
