package io.agora.cruise.analyzer.module;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.NodeRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;

import java.util.List;
import java.util.Set;

import static io.agora.cruise.core.NodeUtils.findAllFirstLeafNode;

/** NodeRelMeta. */
public class NodeRelMeta {

    public static final NodeRelMeta EMPTY = new NodeRelMeta();

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

    public List<Node<RelNode>> leafNodes() {
        return leafNodes;
    }

    /**
     * at least contains one value from from to to.
     *
     * @param from from
     * @param to to
     * @return boolean
     */
    public static boolean contains(NodeRelMeta from, NodeRelMeta to) {
        for (String a : from.tables) {
            if (to.tables.contains(a)) {
                return true;
            }
        }
        return false;
    }
}
