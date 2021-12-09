package io.agora.cruise.core.merge.rule;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * AggregateFilterFinder.
 *
 * <p>Find the first filter in input RelNode.
 */
public class TopFilterFinder extends RelShuttleImpl {

    private List<Filter> filters = new ArrayList<>();

    /**
     * Find the first filter from input rel node.
     *
     * @param relNode rel node
     * @return filter
     */
    public static List<Filter> find(RelNode relNode) {
        TopFilterFinder finder = new TopFilterFinder();
        relNode.accept(finder);
        return finder.filters;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        filters.add(filter);
        return super.visit(filter);
    }
}
