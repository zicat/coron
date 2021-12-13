package io.agora.cruise.presto;

import io.agora.cruise.core.NodeRel;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.presto.sql.SqlFilter;
import io.agora.cruise.presto.sql.SqlIterable;
import io.agora.cruise.presto.sql.SqlIterator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PrestoDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findAllSubNode;

/** PublicSqlTool. */
public class SubSqlTool {

    protected final SqlIterable source;
    protected final SqlIterable target;
    protected final NodeRel.Simplify simplify;
    protected final SqlFilter sqlFilter;
    protected final FileContext fileContext;
    protected final SqlShuttle[] sqlShuttles;
    protected final ExceptionHandler handler;

    public SubSqlTool(
            SqlIterable source,
            SqlIterable target,
            NodeRel.Simplify simplify,
            SqlFilter sqlFilter,
            ExceptionHandler handler,
            FileContext fileContext,
            SqlShuttle... sqlShuttles) {
        this.source = source;
        this.target = target;
        this.simplify = simplify;
        this.sqlFilter = sqlFilter;
        this.handler = handler;
        this.fileContext = fileContext;
        this.sqlShuttles = sqlShuttles;
    }

    /**
     * start calculate.
     *
     * @return tuple
     */
    public Tuple2<Set<String>, Map<String, Set<String>>> start() {
        Map<String, Set<String>> notMatch = new HashMap<>();
        Set<String> viewQuerySet = new HashSet<>();
        SqlIterator fromIt = source.sqlIterator();
        while (fromIt.hasNext()) {
            String fromSql = fromIt.next();
            if (fromSql == null || !sqlFilter.filter(fromSql)) {
                continue;
            }
            SqlIterator toIt = target.sqlIterator();
            if (source == target) {
                toIt.skip(fromIt.currentOffset());
            }
            while (toIt.hasNext()) {
                String toSql = toIt.next();
                if (toSql == null || !sqlFilter.filter(toSql) || fromSql.equals(toSql)) {
                    continue;
                }
                try {
                    calculate(fromSql, toSql, notMatch, viewQuerySet);
                } catch (Throwable e) {
                    handler.handle(fromSql, toSql, e);
                }
            }
        }
        return Tuple2.of(viewQuerySet, notMatch);
    }

    /**
     * filter some view query.
     *
     * @param viewQuery viewQuery
     * @return boolean filter
     */
    protected boolean filterMaterializedView(String viewQuery) {
        if (!viewQuery.contains("GROUP BY") && !viewQuery.contains("JOIN")) {
            return true;
        }
        return !viewQuery.contains("WHERE");
    }

    /**
     * calculate 2 sql materialized query.
     *
     * @param fromSql fromSql
     * @param toSql toSql
     * @param notMatch notMath
     * @param allViewSet allViewSet
     * @throws SqlParseException SqlParseException
     */
    private void calculate(
            String fromSql, String toSql, Map<String, Set<String>> notMatch, Set<String> allViewSet)
            throws SqlParseException {

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(fromSql, sqlShuttles);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(toSql, sqlShuttles);
        final RelNode relNode1 = fileContext.sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = fileContext.sqlNode2RelNode(sqlNode2);
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(
                        createNodeRelRoot(relNode1, simplify),
                        createNodeRelRoot(relNode2, simplify));
        if (resultNodeList.isEmpty()) {
            return;
        }

        Set<String> viewNames = new HashSet<>();
        for (ResultNode<RelNode> resultNode : resultNodeList) {
            final String viewQuery =
                    fileContext.toSql(resultNode.getPayload(), PrestoDialect.DEFAULT);
            if (filterMaterializedView(viewQuery)) {
                continue;
            }
            if (allViewSet.contains(viewQuery)) {
                continue;
            }
            final String viewName = "view_" + allViewSet.size();
            allViewSet.add(viewQuery);
            viewNames.add(viewName);
            fileContext.addMaterializedView(viewName, resultNode.getPayload());
        }
        if (viewNames.isEmpty()) {
            return;
        }
        if (fileContext.canMaterialized(relNode1, viewNames).isEmpty()) {
            notMatch.put(fromSql, viewNames);
        }
        fileContext.cleanupView();
    }

    /** ExceptionHandler. */
    public interface ExceptionHandler {

        /**
         * deal with exception.
         *
         * @param sourceSql sourceSql.
         * @param targetSql targetSql.
         * @param e e;
         */
        void handle(String sourceSql, String targetSql, Throwable e);
    }
}
