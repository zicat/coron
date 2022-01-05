package org.apache.calcite.rel;

import org.apache.calcite.rel.core.TableScan;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** TableRelShuttleImpl. */
public class TableRelShuttleImpl extends RelShuttleImpl {

    private Set<String> tables = new HashSet<>();

    @Override
    public RelNode visit(TableScan tableScan) {
        tables.add(String.join(".", tableScan.getTable().getQualifiedName()));
        return tableScan;
    }

    public Set<String> getTables() {
        return tables;
    }

    /**
     * find tables.
     *
     * @param relRoot rel root
     * @return table name
     */
    public static Set<String> tables(RelRoot relRoot) {
        return tables(relRoot.rel);
    }

    /**
     * find tables.
     *
     * @param relNode rel root
     * @return table name
     */
    public static Set<String> tables(RelNode relNode) {
        TableRelShuttleImpl tableRelShuttle = new TableRelShuttleImpl();
        relNode.accept(tableRelShuttle);
        return tableRelShuttle.getTables();
    }

    /**
     * find table string.
     *
     * @param relRoot rel root
     * @return path
     */
    public static String tableString(RelRoot relRoot) {
        return tableString(relRoot.rel);
    }

    /**
     * find table string.
     *
     * @param relNode rel root
     * @return path
     */
    public static String tableString(RelNode relNode) {
        Set<String> tables = tables(relNode);
        return tables == null ? null : formatPath(tables);
    }

    /**
     * format to string.
     *
     * @param sourcePaths source paths
     * @return string path
     */
    public static String formatPath(Set<String> sourcePaths) {
        if (sourcePaths == null) {
            return null;
        }
        return sourcePaths.stream().sorted(Comparator.reverseOrder()).collect(Collectors.joining());
    }
}
