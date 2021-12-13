package io.agora.cruise.presto.test;

import io.agora.cruise.core.NodeRel;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import io.agora.cruise.presto.FileContext;
import io.agora.cruise.presto.SubSqlTool;
import io.agora.cruise.presto.simplify.PartitionAggregateFilterSimplify;
import io.agora.cruise.presto.simplify.PartitionFilterSimplify;
import io.agora.cruise.presto.sql.SqlFilter;
import io.agora.cruise.presto.sql.SqlIterable;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** QueryTestBase. */
public class QueryTestBase extends FileContext {

    List<String> partitionFields = Collections.singletonList("date");
    NodeRel.Simplify simplify =
            new NodeRel.SimplifyChain(
                    Arrays.asList(
                            new PartitionAggregateFilterSimplify(partitionFields),
                            new PartitionFilterSimplify(partitionFields)));

    protected SqlFilter sqlFilter =
            sql ->
                    sql.startsWith("explain")
                            || sql.contains("system.runtime")
                            || sql.toUpperCase().startsWith("SHOW ");
    protected SqlShuttle[] sqlShuttles = new SqlShuttle[] {new Int2BooleanConditionShuttle()};
    protected SubSqlTool.ExceptionHandler exceptionHandler =
            (sourceSql, targetSql, e) -> {
                if (!e.toString().contains("Object 'media' not found")
                        && !e.toString().contains("Object 'queries' not found")
                        && !e.toString().contains("Object 'information_schema' not found")) {
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
                source, target, simplify, sqlFilter, exceptionHandler, this, sqlShuttles) {

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
