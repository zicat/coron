package io.agora.cruise.core.merge.rule;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;

/** TopAggregationFinder. */
public class TopAggregationFinder extends RelShuttleImpl {

    private Aggregate aggregate;

    /**
     * Find the first filter from input rel node.
     *
     * @param relNode rel node
     * @return filter
     */
    public static Aggregate find(RelNode relNode) {
        TopAggregationFinder finder = new TopAggregationFinder();
        relNode.accept(finder);
        return finder.aggregate;
    }

    /**
     * Contains any aggregate in relNode.
     *
     * @param relNode relNode
     * @return boolean
     */
    public static boolean contains(RelNode relNode) {
        return find(relNode) != null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        this.aggregate = aggregate;
        return aggregate;
    }
}
