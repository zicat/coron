package io.agora.cruise.analyzer.rel;

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

    public static RelShuttleChain empty() {
        return new RelShuttleChain(null, null);
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
     * translate RelNode struct by shuttles.
     *
     * @param relNode relNode
     * @return RelNode
     */
    public final RelNode accept(RelNode relNode) {

        if (relNode == null) {
            return null;
        }
        RelShuttleChain offsetChain = this;
        RelNode result = relNode;
        do {
            if (offsetChain.shuttles != null && offsetChain.shuttles.length != 0) {
                for (RelShuttleImpl relShuttle : offsetChain.shuttles) {
                    try {
                        result = result.accept(relShuttle);
                    } catch (RelShuttleChainException chainException) {
                        LOG.warn(
                                "RelShuttle Convert Fail {}, {}",
                                relShuttle.getClass(),
                                chainException.getMessage());
                        return null;
                    } catch (Exception e) {
                        LOG.warn("RelShuttle Convert Fail " + relShuttle.getClass(), e);
                        return null;
                    }
                }
            }
            offsetChain = offsetChain.nextChain;
        } while (offsetChain != null);
        return result;
    }
}