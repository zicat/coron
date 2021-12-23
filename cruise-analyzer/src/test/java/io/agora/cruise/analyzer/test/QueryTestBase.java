package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.FileContext;
import io.agora.cruise.analyzer.SubSqlTool;
import io.agora.cruise.analyzer.shuttle.PartitionRelShuttle;
import io.agora.cruise.analyzer.sql.SqlFilter;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.dialect.PrestoDialect;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.sql.shuttle.HavingCountShuttle;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Collections;
import java.util.List;

/** QueryTestBase. */
public class QueryTestBase extends FileContext {

    protected List<String> partitionFields = Collections.singletonList("date");
    protected RelShuttleChain shuttleChain =
            RelShuttleChain.of(PartitionRelShuttle.partitionShuttles(partitionFields));

    protected SqlFilter sqlFilter =
            sql ->
                    sql.startsWith("explain")
                            || sql.contains("system.runtime")
                            || sql.toUpperCase().startsWith("SHOW ")
                            || sql.contains("levels_usage_dod_di_11");
    protected SqlShuttle[] sqlShuttles =
            new SqlShuttle[] {new Int2BooleanConditionShuttle(), new HavingCountShuttle()};
    protected SubSqlTool.ExceptionHandler exceptionHandler =
            (sql, e) -> {
                if (!e.toString().contains("Object 'media' not found")
                        && !e.toString().contains("Object 'queries' not found")
                        && !e.toString().contains("Object 'information_schema' not found")
                        && !e.toString().contains("Object 'vendor_vid_sku_final_di_1' not found")
                        && !e.toString().contains("not found in any table")) {
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
    public SubSqlTool createSubSqlTool(
            SqlIterable source, SqlIterable target, SqlFilter viewFilter) {
        return new SubSqlTool(
                source,
                target,
                shuttleChain,
                sqlFilter,
                exceptionHandler,
                this,
                PrestoDialect.DEFAULT,
                sqlShuttles) {

            /**
             * filter some view query.
             *
             * @param viewQuery viewQuery
             * @return boolean filter
             */
            protected boolean filterMaterializedView(String viewQuery) {
                if (viewFilter != null) {
                    return viewFilter.filter(viewQuery);
                }
                return super.filterMaterializedView(viewQuery);
            }
        };
    }
}
