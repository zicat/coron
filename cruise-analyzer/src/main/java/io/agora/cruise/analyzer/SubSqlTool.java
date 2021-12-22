package io.agora.cruise.analyzer;

import io.agora.cruise.analyzer.sql.SqlFilter;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.*;

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
    protected final SqlDialect sqlDialect;

    public SubSqlTool(
            SqlIterable source,
            SqlIterable target,
            RelShuttleChain shuttleChain,
            SqlFilter sqlFilter,
            ExceptionHandler handler,
            FileContext calciteContext,
            SqlDialect sqlDialect,
            SqlShuttle... sqlShuttles) {
        this.source = source;
        this.target = target;
        this.shuttleChain = shuttleChain;
        this.sqlFilter = sqlFilter;
        this.handler = handler;
        this.calciteContext = calciteContext;
        this.sqlDialect = sqlDialect;
        this.sqlShuttles = sqlShuttles;
    }

    /**
     * start calculate.
     *
     * @return set
     */
    public List<RelNode> start() {

        final Map<String, RelNode> cache = new HashMap<>();
        final Map<String, RelNode> viewQuerySet = new HashMap<>();
        final SqlIterator fromIt = source.sqlIterator();
        while (fromIt.hasNext()) {
            final String fromSql = fromIt.next();
            if (fromSql == null || sqlFilter.filter(fromSql)) {
                continue;
            }
            final SqlIterator toIt = target.sqlIterator();
            startBeginFrom(fromSql, fromIt.currentOffset(), toIt, viewQuerySet, cache);
        }
        return new ArrayList<>(viewQuerySet.values());
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
            Map<String, RelNode> viewQuerySet,
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
            String fromSql,
            String toSql,
            Map<String, RelNode> allViewSet,
            Map<String, RelNode> cache)
            throws SqlParseException {

        final RelNode relNode1 = getRelNode(fromSql, cache);
        final RelNode relNode2 = getRelNode(toSql, cache);
        if (relNode1 == null || relNode2 == null) {
            return false;
        }
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(
                        createNodeRelRoot(relNode1, shuttleChain),
                        createNodeRelRoot(relNode2, shuttleChain));
        if (resultNodeList.isEmpty()) {
            return false;
        }

        Set<String> viewNames = new HashSet<>();
        for (ResultNode<RelNode> resultNode : resultNodeList) {
            final String viewQuery = calciteContext.toSql(resultNode.getPayload(), sqlDialect);
            if (allViewSet.containsKey(viewQuery)) {
                continue;
            }
            if (filterMaterializedView(viewQuery)) {
                continue;
            }
            final String viewName = "view_" + allViewSet.size();
            allViewSet.put(viewQuery, resultNode.getPayload());
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
        if (cache.containsKey(sql)) {
            return cache.get(sql);
        }
        RelNode relnode = null;
        try {
            relnode = calciteContext.querySql2Rel(sql, sqlShuttles);
            return relnode;
        } finally {
            cache.put(sql, relnode);
        }
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
