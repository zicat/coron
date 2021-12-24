package io.agora.cruise.analyzer;

import io.agora.cruise.analyzer.sql.SqlFilter;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.core.NodeUtils;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.rel.RelShuttleChain;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findAllSubNode;

/** PublicSqlTool. */
public class SubSqlTool {

    private static final Logger LOG = LoggerFactory.getLogger(SubSqlTool.class);

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
            CalciteContext calciteContext,
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
            LOG.info(
                    "current:{}, cache size:{}, view_size:{}",
                    fromIt.currentOffset(),
                    cache.size(),
                    viewQuerySet.size());
            if (filterSql(fromSql)) {
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
        RelNode relNode1;
        try {
            relNode1 = getRelNode(fromSql, cache);
            if (relNode1 == null) {
                return;
            }
        } catch (Throwable e) {
            handler.handle(fromSql, e);
            return;
        }
        Map<String, RelNode> matchResult = new HashMap<>();
        while (toIt.hasNext()) {
            String toSql = toIt.next();
            if (filterSql(toSql) || fromSql.equals(toSql)) {
                continue;
            }
            try {
                Map<String, RelNode> payloads = calculate(relNode1, toSql, cache);
                if (payloads != null) {
                    matchResult.putAll(payloads);
                }
            } catch (Throwable e) {
                handler.handle(toSql, e);
            }
        }
        findBestView(matchResult, viewQuerySet);
    }

    /**
     * filter illegal.
     *
     * @param sql sql
     * @return boolean filter
     */
    protected boolean filterSql(String sql) {
        return sql == null || sqlFilter.filter(sql);
    }

    /**
     * find best view query.
     *
     * @param payloads payloads
     * @param viewQuerySet viewQuerySet
     */
    protected void findBestView(Map<String, RelNode> payloads, Map<String, RelNode> viewQuerySet) {

        if (payloads == null || payloads.isEmpty()) {
            return;
        }
        Set<Integer> deepLadder = new HashSet<>();
        Map<String, RelNode> bestSqlMap = new HashMap<>();
        for (Map.Entry<String, RelNode> entry : payloads.entrySet()) {
            final String viewSql = entry.getKey();
            final RelNode viewRelNode = entry.getValue();
            if (!viewSql.contains("WHERE ") || viewQuerySet.containsKey(viewSql)) {
                continue;
            }
            final int deep = NodeUtils.deep(viewRelNode);
            if (!deepLadder.contains(deep) && !bestSqlMap.containsKey(viewSql)) {
                deepLadder.add(deep);
                bestSqlMap.put(viewSql, viewRelNode);
            }
        }
        for (Map.Entry<String, RelNode> bestSql : bestSqlMap.entrySet()) {
            viewQuerySet.put(viewName(viewQuerySet.size()), bestSql.getValue());
        }
    }

    /**
     * create viewName by id.
     *
     * @param viewId viewId
     * @return viewName
     */
    protected String viewName(int viewId) {
        return "view_" + viewId;
    }

    /**
     * calculate 2 sql materialized query.
     *
     * @param fromNode fromNode
     * @param toSql toSql
     * @throws SqlParseException SqlParseException
     */
    private Map<String, RelNode> calculate(
            RelNode fromNode, String toSql, Map<String, RelNode> cache) throws SqlParseException {

        final RelNode toNode = getRelNode(toSql, cache);
        if (toNode == null) {
            return null;
        }
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(createNodeRelRoot(fromNode), createNodeRelRoot(toNode));
        if (resultNodeList.isEmpty()) {
            return null;
        }

        Map<String, RelNode> payloadResult = new HashMap<>();
        for (ResultNode<RelNode> resultNode : resultNodeList) {
            final String viewQuery = calciteContext.toSql(resultNode.getPayload(), sqlDialect);
            payloadResult.put(viewQuery, resultNode.getPayload());
        }
        return payloadResult;
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
            relnode = shuttleChain.accept(calciteContext.querySql2Rel(sql, sqlShuttles));
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
         * @param sql sql.
         * @param e e;
         */
        void handle(String sql, Throwable e);
    }
}
