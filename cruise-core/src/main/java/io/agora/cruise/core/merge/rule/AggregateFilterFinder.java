package io.agora.cruise.core.merge.rule;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;

/** AggregateFilterFinder. */
public class AggregateFilterFinder extends RelShuttleImpl {

    private Filter filter;

    public static Filter find(RelNode relNode) {
        AggregateFilterFinder finder = new AggregateFilterFinder();
        relNode.accept(finder);
        return finder.filter;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        this.filter = filter;
        return filter;
    }
}
