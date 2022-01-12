package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.FileContext;
import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.rel.RelShuttleChain;
import io.agora.cruise.analyzer.sql.SqlFilter;
import io.agora.cruise.analyzer.sql.dialect.PrestoDialect;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Collections;
import java.util.List;

import static io.agora.cruise.analyzer.shuttle.PartitionRelShuttle.partitionShuttles;

/** QueryTestBase. */
public class QueryTestBase extends FileContext {

    protected SqlFilter sqlFilter =
            sql ->
                    sql.startsWith("explain")
                            || sql.contains("system.runtime")
                            || sql.startsWith("SHOW ")
                            || sql.startsWith("Show ")
                            || sql.startsWith("show ")
                            || sql.contains("levels_usage_dod_di_11");
    protected SqlAnalyzer.ExceptionHandler exceptionHandler =
            (sql, e) -> {
                String eString = e.toString();
                if (!eString.contains("Object 'media' not found")
                        && !eString.contains("Object 'queries' not found")
                        && !eString.contains("Object 'information_schema' not found")
                        && !eString.contains("Object 'vendor_vid_sku_final_di_1' not found")
                        && !eString.contains("not found in any table")
                        && !eString.contains("Column 'date' is ambiguous")) {
                    e.printStackTrace();
                }
            };

    public QueryTestBase() {
        super("report_datahub");
    }

    /**
     * create SubSql Tool.
     *
     * @return SubSqlTool
     */
    public SqlAnalyzer createSqlAnalyzer() {
        return new SqlAnalyzer(sqlFilter, exceptionHandler, this, PrestoDialect.DEFAULT) {

            @Override
            protected RelShuttleChain createShuttleChain(RelNode relNode) {
                List<String> partitionFields = Collections.singletonList("date");
                return RelShuttleChain.of(partitionShuttles(partitionFields));
            }

            @Override
            protected SqlShuttle[] createSqlShuttle(String sql) {
                return new SqlShuttle[] {new Int2BooleanConditionShuttle()};
            }
        };
    }
}
