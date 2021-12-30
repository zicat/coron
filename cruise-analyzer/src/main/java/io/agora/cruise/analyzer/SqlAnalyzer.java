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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.agora.cruise.analyzer.module.NodeRelMeta.EMPTY;
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
        return start(Runtime.getRuntime().availableProcessors());
    }

    /**
     * start calculate.
     *
     * @param threadCount threadCount
     * @return Map
     */
    public Map<String, RelNode> start(int threadCount) {

        final Map<String, NodeRelMeta> cache = new ConcurrentHashMap<>();
        final Map<String, RelNode> viewQuerySet = new TreeMap<>();
        final Metrics metrics = new Metrics();
        final ExecutorService service = Executors.newFixedThreadPool(threadCount);
        final int blockSize = threadCount * 2;
        try {
            LOG.info("start building from sql node");
            final List<NodeRelMeta> fMeta = buildMetas(source, cache, service, blockSize, metrics);
            LOG.info("start building target sql node");
            final List<NodeRelMeta> tMeta = buildMetas(target, cache, service, blockSize, metrics);
            LOG.info("start found view query, {}", metrics);
            for (int i = 0; i < fMeta.size(); i++) {
                final List<Future<ResultNodeList<RelNode>>> fs = new ArrayList<>(blockSize);
                final NodeRelMeta fromNodeRelMeta = fMeta.get(i);
                final Map<String, RelNode> matchResult = new TreeMap<>();
                for (int j = source == target ? i : 0; j < tMeta.size(); j++) {
                    final NodeRelMeta toNodeRelMeta = tMeta.get(j);
                    final Future<ResultNodeList<RelNode>> future =
                            service.submit(
                                    () -> calculate(fromNodeRelMeta, toNodeRelMeta, metrics));
                    fs.add(future);
                    if (fs.size() >= blockSize) {
                        resultNodeListGet(fs, metrics, matchResult);
                    }
                }
                resultNodeListGet(fs, metrics, matchResult);
                findBestView(matchResult, viewQuerySet);
                if (i % threadCount == 0) {
                    LOG.info("current_query:{}, view_size:{}, {}", i, viewQuerySet.size(), metrics);
                }
            }
            LOG.info("finish match, view_size:{}, {}", viewQuerySet.size(), metrics);
            return viewQuerySet;
        } finally {
            service.shutdownNow();
        }
    }

    /**
     * resultNodeListFutureGet.
     *
     * @param fs future List
     * @param metrics metrics
     * @param matchResult matchResult
     */
    private void resultNodeListGet(
            List<Future<ResultNodeList<RelNode>>> fs,
            Metrics metrics,
            Map<String, RelNode> matchResult) {
        for (Future<ResultNodeList<RelNode>> future : fs) {
            final Map<String, RelNode> payloadResult = new TreeMap<>();
            try {
                final ResultNodeList<RelNode> resultNodeList = future.get();
                final long start = System.currentTimeMillis();
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
        fs.clear();
    }

    /**
     * buildNodeRelMetaList.
     *
     * @param sqlIterable sqlIterable
     * @param cache cache
     * @param service service
     * @return List
     */
    private List<NodeRelMeta> buildMetas(
            final SqlIterable sqlIterable,
            Map<String, NodeRelMeta> cache,
            ExecutorService service,
            int blockSize,
            Metrics metrics) {

        final List<NodeRelMeta> result = new ArrayList<>();
        final List<Future<NodeRelMeta>> fs = new ArrayList<>(blockSize);
        final SqlIterator it = sqlIterable.sqlIterator();
        while (it.hasNext()) {
            final String sql = it.next();
            if (filterSql(sql)) {
                continue;
            }
            fs.add(service.submit(() -> getRelNode(sql, cache, metrics)));
            if (fs.size() >= blockSize) {
                nodeRelMetaGet(fs, result);
            }
        }
        nodeRelMetaGet(fs, result);
        return result;
    }

    /**
     * nodeRelMetaGet.
     *
     * @param fs fs
     * @param metaList metaList
     */
    private void nodeRelMetaGet(List<Future<NodeRelMeta>> fs, List<NodeRelMeta> metaList) {
        for (Future<NodeRelMeta> future : fs) {
            try {
                NodeRelMeta nodeRelMeta = future.get();
                if (nodeRelMeta != EMPTY) {
                    metaList.add(nodeRelMeta);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        fs.clear();
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
        final long start = System.currentTimeMillis();
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
        NodeRelMeta nodeRelMeta = EMPTY;
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
