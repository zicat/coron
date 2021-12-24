package io.agora.cruise.core.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RelShuttleChain. */
public class RelShuttleChain {

    private static final Logger LOG = LoggerFactory.getLogger(RelShuttleChain.class);

    private final RelShuttleImpl[] shuttles;
    private final RelShuttleChain nextChain;

    public RelShuttleChain(RelShuttleImpl[] shuttles, RelShuttleChain nextChain) {
        this.shuttles = shuttles;
        this.nextChain = nextChain;
    }

    /**
     * create RelShuttleChain.
     *
     * @param shuttles shuttles
     * @return RelShuttleChain
     */
    public static RelShuttleChain of(RelShuttleImpl... shuttles) {
        return new RelShuttleChain(shuttles, null);
    }

    /**
     * create RelShuttleChain.
     *
     * @param shuttles shuttles
     * @return RelShuttleChain
     */
    public static RelShuttleChain of(RelShuttleChain nextChain, RelShuttleImpl... shuttles) {
        return new RelShuttleChain(shuttles, nextChain);
    }

    /**
     * empty chain.
     *
     * @return RelShuttleChain
     */
    public static RelShuttleChain empty() {
        return new RelShuttleChain(null, null);
    }

    /**
     * translate RelNode struct by shuttles.
     *
     * @param relNode relNode
     * @return RelNode
     */
    public final RelNode accept(RelNode relNode) {

        RelShuttleChain offsetChain = this;
        RelNode result = relNode;
        do {
            if (offsetChain.shuttles != null && offsetChain.shuttles.length != 0) {
                RelNode tmp = result;
                for (RelShuttleImpl relShuttle : offsetChain.shuttles) {
                    try {
                        result = result.accept(relShuttle);
                    } catch (RelShuttleChainException e) {
                        LOG.warn(relShuttle.getClass().getName() + ":" + e.getMessage());
                        result = tmp;
                        break;
                    }
                }
            }
            offsetChain = offsetChain.nextChain;
        } while (offsetChain != null);
        return result;
    }
}
