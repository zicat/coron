package io.agora.cruise.analyzer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.agora.cruise.analyzer.metrics.AnalyzerSpend;
import io.agora.cruise.analyzer.module.NodeRelMeta;
import io.agora.cruise.analyzer.rel.RelShuttleChain;
import io.agora.cruise.analyzer.sql.SqlFilter;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.util.RelNodeUtils;
import io.agora.cruise.core.NodeUtils;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findAllSubNode;

/** SqlAnalyzer. */
public class SqlAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(SqlAnalyzer.class);
    private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final List<Class<?>> DEFAULT_NODE_RESERVE_LIST =
            Arrays.asList(Filter.class, Aggregate.class, Join.class);
    private static final ExceptionHandler DEFAULT_EXCEPTION_HANDLER = LOG::error;
    private static final SqlFilter DEFAULT_SQL_FILTER = sql -> false;

    protected final SqlFilter sqlFilter;
    protected final CalciteContext context;
    protected final ExceptionHandler handler;
    protected final SqlDialect sqlDialect;

    public SqlAnalyzer(CalciteContext context, SqlDialect sqlDialect) {
        this(DEFAULT_SQL_FILTER, DEFAULT_EXCEPTION_HANDLER, context, sqlDialect);
    }

    public SqlAnalyzer(
            SqlFilter sqlFilter,
            ExceptionHandler handler,
            CalciteContext context,
            SqlDialect sqlDialect) {
        this.sqlFilter = sqlFilter;
        this.handler = handler;
        this.context = context;
        this.sqlDialect = sqlDialect;
    }

    /**
     * start analyze with default threads.
     *
     * @param source source sql
     * @param target target sql
     * @return Map
     */
    public Map<String, RelNode> analyze(SqlIterable source, SqlIterable target) {
        return analyze(source, target, DEFAULT_THREAD_COUNT);
    }

    /**
     * start analyze.
     *
     * @param source source sql
     * @param target target sql
     * @param threadCount threadCount
     * @return Map
     */
    public Map<String, RelNode> analyze(SqlIterable source, SqlIterable target, int threadCount) {

        final Map<String, NodeRelMeta> cache = new ConcurrentHashMap<>();
        final Map<String, RelNode> viewQuerySet = new TreeMap<>();
        final AnalyzerSpend spend = new AnalyzerSpend();
        final ExecutorService service = Executors.newFixedThreadPool(threadCount);
        try {
            final int blockSize = threadCount * 2;
            final List<NodeRelMeta> fMetas = buildMetas(source, cache, service, blockSize, spend);
            final List<NodeRelMeta> tMetas = buildMetas(target, cache, service, blockSize, spend);
            LOG.info("start found view query, {}", spend);
            for (int i = 0; i < fMetas.size(); i++) {
                final List<Future<ResultNodeList<RelNode>>> fs = new ArrayList<>(blockSize);
                final NodeRelMeta fMeta = fMetas.get(i);
                final Map<String, RelNode> matchResult = new TreeMap<>();
                for (int j = source == target ? i + 1 : 0; j < tMetas.size(); j++) {
                    final NodeRelMeta tMeta = tMetas.get(j);
                    fs.add(service.submit(() -> analyze(fMeta, tMeta, spend)));
                    if (fs.size() >= blockSize) {
                        matchResult.putAll(resultNodeListGet(fs, spend));
                    }
                }
                matchResult.putAll(resultNodeListGet(fs, spend));
                final Collection<RelNode> bestViews = findBestView(matchResult, viewQuerySet);
                bestViews.forEach(n -> viewQuerySet.put(viewName(viewQuerySet.size()), n));
                LOG.debug("current_query:{}, view_size:{}, {}", i, viewQuerySet.size(), spend);
            }
            LOG.info("finish match, view_size:{}, {}", viewQuerySet.size(), spend);
            return viewQuerySet;
        } finally {
            service.shutdownNow();
        }
    }

    /**
     * resultNodeListFutureGet.
     *
     * @param fs future List
     * @param analyzerSpend metrics
     */
    private Map<String, RelNode> resultNodeListGet(
            List<Future<ResultNodeList<RelNode>>> fs, AnalyzerSpend analyzerSpend) {

        final Map<String, RelNode> result = Maps.newHashMapWithExpectedSize(fs.size());
        for (Future<ResultNodeList<RelNode>> future : fs) {
            try {
                final ResultNodeList<RelNode> resultNodeList = future.get();
                final long start = System.currentTimeMillis();
                for (ResultNode<RelNode> resultNode : resultNodeList) {
                    final String viewQuery = context.toSql(resultNode.getPayload(), sqlDialect);
                    result.put(viewQuery, resultNode.getPayload());
                }
                analyzerSpend.addTotalNode2SqlSpend(System.currentTimeMillis() - start);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        fs.clear();
        return result;
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
            AnalyzerSpend analyzerSpend) {

        LOG.info("start building sql node");
        final List<NodeRelMeta> result = new ArrayList<>(sqlIterable.size());
        final List<Future<NodeRelMeta>> fs = new ArrayList<>(blockSize);
        final SqlIterator it = sqlIterable.sqlIterator();
        while (it.hasNext()) {
            final String sql = it.next();
            if (filterSql(sql)) {
                continue;
            }
            fs.add(service.submit(() -> getRelNode(sql, cache, analyzerSpend)));
            if (fs.size() >= blockSize) {
                result.addAll(nodeRelMetaGet(fs));
            }
        }
        result.addAll(nodeRelMetaGet(fs));
        return result;
    }

    /**
     * nodeRelMetaGet.
     *
     * @param fs fs
     */
    private List<NodeRelMeta> nodeRelMetaGet(List<Future<NodeRelMeta>> fs) {

        final List<NodeRelMeta> result = new ArrayList<>(fs.size());
        for (Future<NodeRelMeta> future : fs) {
            try {
                NodeRelMeta nodeRelMeta = future.get();
                if (!nodeRelMeta.isEmpty()) {
                    result.add(nodeRelMeta);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        fs.clear();
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
    protected Collection<RelNode> findBestView(
            Map<String, RelNode> payloads, Map<String, RelNode> viewQuerySet) {

        final List<RelNode> bestViews = new ArrayList<>();
        if (payloads == null || payloads.isEmpty()) {
            return bestViews;
        }
        final Set<Integer> deepLadder = new TreeSet<>();
        final Set<String> distinctView = new TreeSet<>();
        for (Map.Entry<String, RelNode> entry : payloads.entrySet()) {
            final String viewSql = entry.getKey();
            final RelNode viewRelNode = entry.getValue();
            if (filterView(viewRelNode)) {
                continue;
            }
            if (viewQuerySet.containsKey(entry.getKey())) {
                continue;
            }
            final int deep = NodeUtils.deep(viewRelNode);
            if (!deepLadder.contains(deep) && !distinctView.contains(viewSql)) {
                deepLadder.add(deep);
                distinctView.add(viewSql);
                bestViews.add(viewRelNode);
            }
        }
        return bestViews;
    }

    /**
     * filter view by viewSql.
     *
     * @param viewQueryRelNode viewQueryRelNode
     * @return filter if return true
     */
    protected boolean filterView(RelNode viewQueryRelNode) {
        return (!RelNodeUtils.containsKind(viewQueryRelNode, DEFAULT_NODE_RESERVE_LIST));
    }

    /**
     * create viewName by id.
     *
     * @param viewId viewId
     * @return viewName
     */
    @VisibleForTesting
    public String viewName(int viewId) {
        return context.defaultDatabase() + ".view_" + viewId;
    }

    /**
     * calculate 2 sql materialized query.
     *
     * @param fromNodeRelMeta fromNodeRelMeta
     * @param toNodeRelMeta toNodeRelMeta
     * @param analyzerSpend metrics
     * @return subMap
     */
    private ResultNodeList<RelNode> analyze(
            NodeRelMeta fromNodeRelMeta, NodeRelMeta toNodeRelMeta, AnalyzerSpend analyzerSpend) {

        if (toNodeRelMeta == null
                || !NodeRelMeta.isTableIntersect(fromNodeRelMeta, toNodeRelMeta)) {
            return new ResultNodeList<>();
        }
        final long start = System.currentTimeMillis();
        final ResultNodeList<RelNode> resultNodeList =
                findAllSubNode(
                        fromNodeRelMeta.nodeRel(),
                        fromNodeRelMeta.leafNodes(),
                        toNodeRelMeta.leafNodes());
        analyzerSpend.addTotalSubSqlSpend(System.currentTimeMillis() - start);
        return resultNodeList;
    }

    /**
     * get RelNode.
     *
     * @param sql sql
     * @return RelNode
     */
    private NodeRelMeta getRelNode(
            String sql, Map<String, NodeRelMeta> cache, AnalyzerSpend analyzerSpend) {

        NodeRelMeta nodeRelMeta;
        if (cache != null && (nodeRelMeta = cache.get(sql)) != null) {
            return nodeRelMeta;
        }

        final long start = System.currentTimeMillis();
        nodeRelMeta = NodeRelMeta.empty();
        try {
            final RelNode relNode = context.querySql2Rel(sql, createSqlShuttle(sql));
            final RelNode chainedRelNode = createShuttleChain(relNode).accept(relNode);
            if (chainedRelNode != null) {
                nodeRelMeta = new NodeRelMeta(createNodeRelRoot(chainedRelNode));
            }
        } catch (Exception e) {
            handler.handle(sql, e);
        } finally {
            if (cache != null) {
                cache.put(sql, nodeRelMeta);
            }
            analyzerSpend.addTotalSql2NodeSpend(System.currentTimeMillis() - start);
        }
        return nodeRelMeta;
    }

    /**
     * create Sql Shuttle.
     *
     * @return SqlShuttle[]
     */
    protected SqlShuttle[] createSqlShuttle(String sql) {
        return null;
    }

    /**
     * create shuttle Chain.
     *
     * @return RelShuttleChain.
     */
    protected RelShuttleChain createShuttleChain(RelNode relNode) {
        return RelShuttleChain.empty();
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
