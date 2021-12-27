package io.agora.cruise.analyzer;

import io.agora.cruise.analyzer.metrics.Metrics;
import io.agora.cruise.analyzer.module.NodeRelMeta;
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
public class SqlAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(SqlAnalyzer.class);

    protected final SqlIterable source;
    protected final SqlIterable target;
    protected final RelShuttleChain shuttleChain;
    protected final SqlFilter sqlFilter;
    protected final CalciteContext calciteContext;
    protected final SqlShuttle[] sqlShuttles;
    protected final ExceptionHandler handler;
    protected final SqlDialect sqlDialect;
    static ThreadLocal<Metrics> localMetrics = new ThreadLocal<>();

    public SqlAnalyzer(
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
     * start to.
     *
     * @return map.
     */
    public Map<String, RelNode> start() {
        return start(CheckMode.FULL);
    }

    /**
     * start calculate.
     *
     * @return map
     */
    public Map<String, RelNode> start(CheckMode checkMode) {

        final Map<String, NodeRelMeta> cache = new TreeMap<>();
        final Map<String, RelNode> viewQuerySet = new TreeMap<>();
        final SqlIterator fromIt = source.sqlIterator();
        final Metrics metrics = new Metrics();
        try {
            localMetrics.set(metrics);
            while (fromIt.hasNext()) {
                long start = System.currentTimeMillis();
                final String fromSql = fromIt.next();
                LOG.info(
                        "current:{}, cache size:{}, view_size:{}, {}",
                        fromIt.currentOffset(),
                        cache.size(),
                        viewQuerySet.size(),
                        metrics);
                if (filterSql(fromSql)) {
                    continue;
                }
                final SqlIterator toIt = target.sqlIterator();
                startBeginFrom(
                        fromSql, fromIt.currentOffset(), toIt, viewQuerySet, cache, checkMode);
                metrics.addTotalSpend(System.currentTimeMillis() - start);
            }
            return viewQuerySet;
        } finally {
            localMetrics.remove();
        }
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
            Map<String, NodeRelMeta> cache,
            CheckMode checkMode) {

        if (source == target) {
            toIt.skip(currentFromOffset);
        }
        NodeRelMeta nodeRelMeta;
        try {
            long start = System.currentTimeMillis();
            nodeRelMeta = getRelNode(fromSql, cache);
            localMetrics.get().addTotalSql2NodeSpend(System.currentTimeMillis() - start);
            if (nodeRelMeta == null) {
                return;
            }
        } catch (Throwable e) {
            handler.handle(fromSql, e);
            return;
        }
        final Map<String, RelNode> matchResult = new TreeMap<>();
        while (toIt.hasNext()) {
            String toSql = toIt.next();
            if (filterSql(toSql) || fromSql.equals(toSql)) {
                continue;
            }
            try {
                final Map<String, RelNode> payloads = calculate(nodeRelMeta, toSql, cache);
                if (payloads != null) {
                    matchResult.putAll(payloads);
                    if (checkMode == CheckMode.SIMPLE) {
                        break;
                    }
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
        final Set<Integer> deepLadder = new TreeSet<>();
        final Map<String, RelNode> bestSqlMap = new TreeMap<>();
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
        return calciteContext.defaultDatabase() + ".view_" + viewId;
    }

    /**
     * calculate 2 sql materialized query.
     *
     * @param fromNodeRelMeta fromNodeRelMeta
     * @param toSql toSql
     * @throws SqlParseException SqlParseException
     */
    private Map<String, RelNode> calculate(
            NodeRelMeta fromNodeRelMeta, String toSql, Map<String, NodeRelMeta> cache)
            throws SqlParseException {

        long start = System.currentTimeMillis();
        final NodeRelMeta toNodeRelMeta = getRelNode(toSql, cache);
        localMetrics.get().addTotalSql2NodeSpend(System.currentTimeMillis() - start);
        if (toNodeRelMeta == null || !NodeRelMeta.contains(fromNodeRelMeta, toNodeRelMeta)) {
            return null;
        }
        start = System.currentTimeMillis();
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(
                        fromNodeRelMeta.nodeRel(),
                        fromNodeRelMeta.leafNodes(),
                        toNodeRelMeta.leafNodes());
        localMetrics.get().addTotalSubSqlSpend(System.currentTimeMillis() - start);
        if (resultNodeList.isEmpty()) {
            return null;
        }

        start = System.currentTimeMillis();
        Map<String, RelNode> payloadResult = new HashMap<>();
        for (ResultNode<RelNode> resultNode : resultNodeList) {
            final String viewQuery = calciteContext.toSql(resultNode.getPayload(), sqlDialect);
            payloadResult.put(viewQuery, resultNode.getPayload());
        }
        localMetrics.get().addTotalNode2SqlSpend(System.currentTimeMillis() - start);
        return payloadResult;
    }

    /**
     * get RelNode.
     *
     * @param sql sql
     * @return RelNode
     * @throws SqlParseException SqlParseException
     */
    private NodeRelMeta getRelNode(String sql, Map<String, NodeRelMeta> cache)
            throws SqlParseException {
        if (cache.containsKey(sql)) {
            return cache.get(sql);
        }
        NodeRelMeta nodeRelMeta = null;
        try {
            final RelNode relnode =
                    shuttleChain.accept(calciteContext.querySql2Rel(sql, sqlShuttles));
            if (relnode != null) {
                nodeRelMeta = new NodeRelMeta(createNodeRelRoot(relnode));
            }
            return nodeRelMeta;
        } finally {
            cache.put(sql, nodeRelMeta);
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

    /** CheckMode. */
    public enum CheckMode {
        SIMPLE,
        FULL
    }
}
