package io.agora.cruise.core.merge.rule;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * AggregateFilterFinder.
 *
 * <p>Find the first filter in input RelNode.
 */
public class TopFilterFinder extends RelShuttleImpl {

    private Filter filter;

    /**
     * Find the first filter from input rel node.
     *
     * @param relNode rel node
     * @return filter
     */
    public static Filter find(RelNode relNode) {
        TopFilterFinder finder = new TopFilterFinder();
        relNode.accept(finder);
        return finder.filter;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        this.filter = filter;
        return filter;
    }
}