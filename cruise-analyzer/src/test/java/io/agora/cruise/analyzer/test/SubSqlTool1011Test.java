package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SubSqlTool;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** SubSqlTool1113Test. */
public class SubSqlTool1011Test {
    public static void main(String[] args) throws IOException {

        File output = new File("output/view_query_2.sql");
        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
        SqlIterable source = new SqlJsonIterable("query_11.json", parser);
        SqlIterable target = new SqlJsonIterable("query_10.json", parser);

        QueryTestBase queryTestBase = new QueryTestBase();
        SubSqlTool subSqlTool =
                queryTestBase.createSubSqlTool(source, target, sql -> !sql.contains("WHERE"));
        Set<String> viewQuerySet = subSqlTool.start();
        List<String> viewQuery =
                viewQuerySet.stream()
                        .map(v -> v.replace("\n", " ").replace("\r", " "))
                        .collect(Collectors.toList());
        FileUtils.writeLines(output, viewQuery, false);
    }
}
