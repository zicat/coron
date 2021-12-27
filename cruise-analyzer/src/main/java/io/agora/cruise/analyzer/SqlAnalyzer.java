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
import org.apache.calcite.sql.util.SqlShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

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
     * start calculate with default threads.
     *
     * @return Map
     */
    public Map<String, RelNode> start() {
        return start(50);
    }

    /**
     * start calculate.
     *
     * @param threadCount threadCount
     * @return Map
     */
    public Map<String, RelNode> start(int threadCount) {

        final Map<String, NodeRelMeta> cache = new TreeMap<>();
        final Map<String, RelNode> viewQuerySet = new TreeMap<>();
        final Metrics metrics = new Metrics();
        final ExecutorService service =
                new ThreadPoolExecutor(
                        threadCount,
                        threadCount,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>());
        try {
            final SqlIterator fromIt = source.sqlIterator();
            final List<NodeRelMeta> fromNodeRelMetaList =
                    buildNodeRelMetaList(fromIt, cache, metrics, "building from sql RelNode ");
            final SqlIterator toIt = target.sqlIterator();
            final List<NodeRelMeta> toNodeRelMetaList =
                    buildNodeRelMetaList(toIt, cache, metrics, "building to sql RelNode ");
            for (int i = 0; i < fromNodeRelMetaList.size(); i++) {
                final List<Future<ResultNodeList<RelNode>>> fs = new ArrayList<>();
                final NodeRelMeta fromNodeRelMeta = fromNodeRelMetaList.get(i);
                final Map<String, RelNode> matchResult = new TreeMap<>();
                LOG.info("current:{}, view_size:{}, {}", i, viewQuerySet.size(), metrics);
                for (int j = source == target ? i : 0; j < toNodeRelMetaList.size(); j++) {
                    final NodeRelMeta toNodeRelMeta = toNodeRelMetaList.get(j);
                    fs.add(
                            service.submit(
                                    () -> calculate(fromNodeRelMeta, toNodeRelMeta, metrics)));
                }
                for (Future<ResultNodeList<RelNode>> f : fs) {
                    Map<String, RelNode> payloadResult = new TreeMap<>();
                    try {
                        ResultNodeList<RelNode> resultNodeList = f.get();
                        long start = System.currentTimeMillis();
                        for (ResultNode<RelNode> resultNode : resultNodeList) {
                            final String viewQuery =
                                    calciteContext.toSql(resultNode.getPayload(), sqlDialect);
                            payloadResult.put(viewQuery, resultNode.getPayload());
                        }
                        metrics.addTotalNode2SqlSpend(System.currentTimeMillis() - start);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    matchResult.putAll(payloadResult);
                }
                findBestView(matchResult, viewQuerySet);
            }
            return viewQuerySet;
        } finally {
            service.shutdownNow();
        }
    }

    /**
     * buildNodeRelMetaList.
     *
     * @param it it
     * @param cache cache
     * @param logMessage logMessage
     * @return List
     */
    private List<NodeRelMeta> buildNodeRelMetaList(
            final SqlIterator it,
            Map<String, NodeRelMeta> cache,
            Metrics metrics,
            String logMessage) {

        List<NodeRelMeta> result = new ArrayList<>();
        while (it.hasNext()) {
            String sql = it.next();
            LOG.info(logMessage + it.currentOffset());
            if (filterSql(sql)) {
                continue;
            }
            NodeRelMeta nodeRelMeta = getRelNode(sql, cache, metrics);
            if (nodeRelMeta != null) {
                result.add(nodeRelMeta);
            }
        }
        return result;
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
     * @param toNodeRelMeta toNodeRelMeta
     * @param metrics metrics
     * @return subMap
     */
    private ResultNodeList<RelNode> calculate(
            NodeRelMeta fromNodeRelMeta, NodeRelMeta toNodeRelMeta, Metrics metrics) {

        if (toNodeRelMeta == null || !NodeRelMeta.contains(fromNodeRelMeta, toNodeRelMeta)) {
            return new ResultNodeList<>();
        }
        long start = System.currentTimeMillis();
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(
                        fromNodeRelMeta.nodeRel(),
                        fromNodeRelMeta.leafNodes(),
                        toNodeRelMeta.leafNodes());
        metrics.addTotalSubSqlSpend(System.currentTimeMillis() - start);
        return resultNodeList;
    }

    /**
     * get RelNode.
     *
     * @param sql sql
     * @return RelNode
     */
    private NodeRelMeta getRelNode(String sql, Map<String, NodeRelMeta> cache, Metrics metrics) {
        if (cache.containsKey(sql)) {
            return cache.get(sql);
        }
        NodeRelMeta nodeRelMeta = null;
        final long start = System.currentTimeMillis();
        try {

            final RelNode relnode =
                    shuttleChain.accept(calciteContext.querySql2Rel(sql, sqlShuttles));
            if (relnode != null) {
                nodeRelMeta = new NodeRelMeta(createNodeRelRoot(relnode));
            }
        } catch (Exception e) {
            handler.handle(sql, e);
        } finally {
            cache.put(sql, nodeRelMeta);
        }
        metrics.addTotalSql2NodeSpend(System.currentTimeMillis() - start);
        return nodeRelMeta;
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
