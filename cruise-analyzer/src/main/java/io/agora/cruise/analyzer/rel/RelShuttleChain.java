package io.agora.cruise.analyzer.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RelShuttleChain. */
public class RelShuttleChain {

    private static final Logger LOG = LoggerFactory.getLogger(RelShuttleChain.class);

    private final RelShuttleImpl[] shuttles;

    public RelShuttleChain(RelShuttleImpl[] shuttles) {
        this.shuttles = shuttles;
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
     * create RelShuttleChain.
     *
     * @param shuttles shuttles
     * @return RelShuttleChain
     */
    public static RelShuttleChain of(RelShuttleImpl... shuttles) {
        return new RelShuttleChain(shuttles);
    }

    /**
     * translate RelNode struct by shuttles.
     *
     * @param relNode relNode
     * @return RelNode, null if accept error
     */
    public final RelNode accept(RelNode relNode) {

        if (relNode == null || shuttles == null) {
            return relNode;
        }
        RelNode result = relNode;
        for (RelShuttleImpl relShuttle : shuttles) {
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
        return result;
    }
}
