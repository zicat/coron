package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SubSqlTool;
import io.agora.cruise.analyzer.sql.SqlCsvIterable;
import io.agora.cruise.analyzer.sql.SqlIterable;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.agora.cruise.analyzer.sql.SqlCsvIterator.CsvParser.FIRST_COLUMN;

/** SubSqlToolTest. */
public class SubSqlToolTest extends QueryTestBase {

    public static void main(String[] args) throws IOException {
        File output = new File("output/view_query.sql");
        SqlIterable sqlIterable = new SqlCsvIterable("query2.log", FIRST_COLUMN);

        QueryTestBase queryTestBase = new QueryTestBase();
        SubSqlTool subSqlTool = queryTestBase.createSubSqlTool(sqlIterable, sqlIterable, null);
        Set<String> viewQuerySet = subSqlTool.start();

        List<String> viewQuery =
                viewQuerySet.stream()
                        .map(v -> v.replace("\n", " ").replace("\r", " "))
                        .collect(Collectors.toList());
        FileUtils.writeLines(output, viewQuery, false);
    }
}
