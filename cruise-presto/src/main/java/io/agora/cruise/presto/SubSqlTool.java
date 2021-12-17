package io.agora.cruise.presto;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.rel.RelShuttleChain;
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
    protected final FileContext fileContext;
    protected final SqlShuttle[] sqlShuttles;
    protected final ExceptionHandler handler;
    protected Map<String, RelNode> cache = new HashMap<>();

    public SubSqlTool(
            SqlIterable source,
            SqlIterable target,
            RelShuttleChain shuttleChain,
            SqlFilter sqlFilter,
            ExceptionHandler handler,
            FileContext fileContext,
            SqlShuttle... sqlShuttles) {
        this.source = source;
        this.target = target;
        this.shuttleChain = shuttleChain;
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
    public Set<String> start() {
        Set<String> viewQuerySet = new HashSet<>();
        SqlIterator fromIt = source.sqlIterator();
        while (fromIt.hasNext()) {
            String fromSql = fromIt.next();
            if (fromSql == null || sqlFilter.filter(fromSql)) {
                continue;
            }
            SqlIterator toIt = target.sqlIterator();
            if (source == target) {
                toIt.skip(fromIt.currentOffset());
            }
            while (toIt.hasNext()) {
                String toSql = toIt.next();
                if (toSql == null || sqlFilter.filter(toSql) || fromSql.equals(toSql)) {
                    continue;
                }
                try {
                    if (calculate(fromSql, toSql, viewQuerySet)) {
                        break;
                    }
                } catch (Throwable e) {
                    handler.handle(fromSql, toSql, e);
                }
            }
        }
        return viewQuerySet;
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
    private boolean calculate(String fromSql, String toSql, Set<String> allViewSet)
            throws SqlParseException {

        final RelNode relNode1 = getRelNode(fromSql);
        final RelNode relNode2 = getRelNode(toSql);
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
    private RelNode getRelNode(String sql) throws SqlParseException {
        RelNode relnode = cache.get(sql);
        if (relnode == null) {
            relnode = fileContext.querySql2Rel(sql, sqlShuttles);
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
