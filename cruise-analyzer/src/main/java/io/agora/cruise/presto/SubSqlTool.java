package io.agora.cruise.presto;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.presto.sql.SqlFilter;
import io.agora.cruise.presto.sql.SqlIterable;
import io.agora.cruise.presto.sql.SqlIterator;
import org.apache.calcite.rel.RelNode;
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
    protected final RelShuttleChain shuttleChain;
    protected final SqlFilter sqlFilter;
    protected final CalciteContext calciteContext;
    protected final SqlShuttle[] sqlShuttles;
    protected final ExceptionHandler handler;

    public SubSqlTool(
            SqlIterable source,
            SqlIterable target,
            RelShuttleChain shuttleChain,
            SqlFilter sqlFilter,
            ExceptionHandler handler,
            FileContext calciteContext,
            SqlShuttle... sqlShuttles) {
        this.source = source;
        this.target = target;
        this.shuttleChain = shuttleChain;
        this.sqlFilter = sqlFilter;
        this.handler = handler;
        this.calciteContext = calciteContext;
        this.sqlShuttles = sqlShuttles;
    }

    /**
     * start calculate.
     *
     * @return set
     */
    public Set<String> start() {

        final Map<String, RelNode> cache = new HashMap<>();
        final Set<String> viewQuerySet = new HashSet<>();
        final SqlIterator fromIt = source.sqlIterator();
        while (fromIt.hasNext()) {
            final String fromSql = fromIt.next();
            if (fromSql == null || sqlFilter.filter(fromSql)) {
                continue;
            }
            final SqlIterator toIt = target.sqlIterator();
            startBeginFrom(fromSql, fromIt.currentOffset(), toIt, viewQuerySet, cache);
        }
        return viewQuerySet;
    }

    /**
     * start from sql.
     *
     * @param fromSql fromSql
     * @param currentFromOffset currentFromOffset
     * @param toIt toIt
     * @param viewQuerySet viewQuerySet
     * @param cache cache
     */
    private void startBeginFrom(
            String fromSql,
            int currentFromOffset,
            SqlIterator toIt,
            Set<String> viewQuerySet,
            Map<String, RelNode> cache) {
        if (source == target) {
            toIt.skip(currentFromOffset);
        }
        while (toIt.hasNext()) {
            String toSql = toIt.next();
            if (toSql == null || sqlFilter.filter(toSql) || fromSql.equals(toSql)) {
                continue;
            }
            try {
                if (calculate(fromSql, toSql, viewQuerySet, cache)) {
                    break;
                }
            } catch (Throwable e) {
                handler.handle(fromSql, toSql, e);
            }
        }
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
     * @param allViewSet allViewSet
     * @throws SqlParseException SqlParseException
     */
    private boolean calculate(
            String fromSql, String toSql, Set<String> allViewSet, Map<String, RelNode> cache)
            throws SqlParseException {

        final RelNode relNode1 = getRelNode(fromSql, cache);
        final RelNode relNode2 = getRelNode(toSql, cache);
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(
                        createNodeRelRoot(relNode1, shuttleChain),
                        createNodeRelRoot(relNode2, shuttleChain));
        if (resultNodeList.isEmpty()) {
            return false;
        }

        Set<String> viewNames = new HashSet<>();
        for (ResultNode<RelNode> resultNode : resultNodeList) {
            final String viewQuery =
                    calciteContext.toSql(resultNode.getPayload(), PrestoDialect.DEFAULT);
            if (allViewSet.contains(viewQuery)) {
                continue;
            }
            if (filterMaterializedView(viewQuery)) {
                continue;
            }
            final String viewName = "view_" + allViewSet.size();
            allViewSet.add(viewQuery);
            viewNames.add(viewName);
        }
        return !viewNames.isEmpty();
    }

    /**
     * get RelNode.
     *
     * @param sql sql
     * @return RelNode
     * @throws SqlParseException SqlParseException
     */
    private RelNode getRelNode(String sql, Map<String, RelNode> cache) throws SqlParseException {
        RelNode relnode = cache.get(sql);
        if (relnode == null) {
            relnode = calciteContext.querySql2Rel(sql, sqlShuttles);
            cache.put(sql, relnode);
        }
        return relnode;
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
