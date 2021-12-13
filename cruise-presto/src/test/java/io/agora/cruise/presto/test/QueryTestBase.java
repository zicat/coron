package io.agora.cruise.presto.test;

import io.agora.cruise.core.NodeRel;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import io.agora.cruise.presto.FileContext;
import io.agora.cruise.presto.SubSqlTool;
import io.agora.cruise.presto.simplify.PartitionAggregateFilterSimplify;
import io.agora.cruise.presto.sql.SqlFilter;
import io.agora.cruise.presto.sql.SqlIterable;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.Collections;

/** QueryTestBase. */
public class QueryTestBase extends FileContext {

    protected NodeRel.Simplify simplify =
            new PartitionAggregateFilterSimplify(Collections.singletonList("date"));
    protected SqlFilter sqlFilter =
            sql ->
                    !sql.startsWith("explain")
                            && !sql.contains("system.runtime")
                            && !sql.toUpperCase().startsWith("SHOW ");
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
    public SubSqlTool createSubSqlTool(SqlIterable source, SqlIterable target) {
        return new SubSqlTool(
                source, target, simplify, sqlFilter, exceptionHandler, this, sqlShuttles);
    }
}
