package io.agora.cruise.core.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;

/** RelShuttleChain. */
public class RelShuttleChain {

    private final RelShuttleImpl[] shuttles;

    public RelShuttleChain(RelShuttleImpl[] shuttles) {
        this.shuttles = shuttles;
    }

    /**
     * create RelShuttleChain.
     *
     * @param shuttles shuttles
     * @return RelShuttleChain
     */
    public static RelShuttleChain of(RelShuttleImpl... shuttles) {
        return new RelShuttleChain(shuttles);
    }

    /**
     * empty chain.
     *
     * @return RelShuttleChain
     */
    public static RelShuttleChain empty() {
        return new RelShuttleChain(null);
    }

    /**
     * translate RelNode struct by shuttles.
     *
     * @param relNode relNode
     * @return RelNode
     */
    public final RelNode accept(RelNode relNode) {
        if (shuttles == null || shuttles.length == 0) {
            return relNode;
        }
        RelNode result = relNode;
        for (RelShuttleImpl relShuttle : shuttles) {
            result = result.accept(relShuttle);
        }
        return result;
    }
}
