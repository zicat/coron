package io.agora.cruise.analyzer.module;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.NodeRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.agora.cruise.core.NodeUtils.findAllFirstLeafNode;

/** NodeRelMeta. */
public final class NodeRelMeta {

    private static final NodeRelMeta EMPTY = new NodeRelMeta();

    private final NodeRel nodeRel;
    private final Set<String> tables;
    private final List<Node<RelNode>> leafNodes;

    private NodeRelMeta() {
        this.nodeRel = null;
        this.tables = null;
        this.leafNodes = null;
    }

    public NodeRelMeta(NodeRel nodeRel) {
        this.nodeRel = nodeRel;
        this.tables = TableRelShuttleImpl.tables(nodeRel.getPayload());
        this.leafNodes = findAllFirstLeafNode(nodeRel);
    }

    public final NodeRel nodeRel() {
        return nodeRel;
    }

    public final Set<String> tables() {
        return tables;
    }

    public final List<Node<RelNode>> leafNodes() {
        return leafNodes;
    }

    /**
     * create empty instance.
     *
     * @return NodeRelMeta
     */
    public static NodeRelMeta empty() {
        return EMPTY;
    }

    /**
     * check if empty.
     *
     * @return boolean
     */
    public final boolean isEmpty() {
        return this == EMPTY;
    }

    /**
     * intersectTables.
     *
     * @param from from
     * @param to to
     * @return intersectTables
     */
    public static Set<String> intersectTables(NodeRelMeta from, NodeRelMeta to) {
        return intersectTables(from, to, false);
    }

    /**
     * check is isTableIntersect.
     *
     * @param from from
     * @param to to
     * @return isTableIntersect
     */
    public static boolean isTableIntersect(NodeRelMeta from, NodeRelMeta to) {
        return !intersectTables(from, to, true).isEmpty();
    }

    /**
     * intersectTables.
     *
     * @param from from
     * @param to to
     * @param breakWhenFirstMatch break if match first
     * @return intersectTables
     */
    private static Set<String> intersectTables(
            NodeRelMeta from, NodeRelMeta to, boolean breakWhenFirstMatch) {
        Set<String> intersectTables = new HashSet<>();
        for (String a : from.tables) {
            if (to.tables.contains(a)) {
                intersectTables.add(a);
                if (breakWhenFirstMatch) {
                    break;
                }
            }
        }
        return intersectTables;
    }
}
