package io.agora.cruise.core;

import io.agora.cruise.core.merge.RelNodeMergePlanner;
import org.apache.calcite.rel.RelNode;

/** NodeRel. */
public class NodeRel extends Node<RelNode> {

    protected final RelNodeMergePlanner mergePlanner;

    protected NodeRel(RelNodeMergePlanner mergePlanner, Node<RelNode> parent, RelNode payload) {
        super(parent, payload);
        this.mergePlanner = mergePlanner;
    }

    /**
     * create RodeRel.
     *
     * @param mergePlanner mergePlanner
     * @param parent parent
     * @param payload payload
     * @return NodeRel
     */
    public static NodeRel of(
            RelNodeMergePlanner mergePlanner, Node<RelNode> parent, RelNode payload) {
        return new NodeRel(mergePlanner, parent, payload);
    }

    /**
     * create RodeRel.
     *
     * @param mergePlanner mergePlanner
     * @param payload payload
     * @return NodeRel
     */
    public static NodeRel of(RelNodeMergePlanner mergePlanner, RelNode payload) {
        return of(mergePlanner, null, payload);
    }

    /**
     * boolean is similar.
     *
     * @param toNode other node
     * @return boolean
     */
    @Override
    public ResultNode<RelNode> merge(
            Node<RelNode> toNode, ResultNodeList<RelNode> childrenResultNode) {

        if (toNode == null) {
            return ResultNode.of(childrenResultNode);
        }
        return mergePlanner.merge(this, toNode, childrenResultNode);
    }
}
