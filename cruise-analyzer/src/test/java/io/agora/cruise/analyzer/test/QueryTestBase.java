package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.FileContext;
import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.sql.SqlFilter;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.dialect.PrestoDialect;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
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
                            || sql.toUpperCase().startsWith("SHOW ")
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
     * @param source source sql iterable
     * @param target target sql iterable
     * @return SubSqlTool
     */
    public SqlAnalyzer createSubSqlTool(SqlIterable source, SqlIterable target) {
        return new SqlAnalyzer(
                source, target, sqlFilter, exceptionHandler, this, PrestoDialect.DEFAULT) {

            protected RelShuttleChain createShuttleChain() {
                List<String> partitionFields = Collections.singletonList("date");
                return RelShuttleChain.of(partitionShuttles(partitionFields));
            }

            protected SqlShuttle[] createSqlShuttle() {
                return new SqlShuttle[] {new Int2BooleanConditionShuttle()};
            }
        };
    }
}
