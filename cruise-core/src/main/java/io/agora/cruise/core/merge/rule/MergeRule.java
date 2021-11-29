package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/** MergeRule. */
public abstract class MergeRule<F extends RelNode, T extends RelNode> {

    protected final MergeConfig<F, T> mergeConfig;

    public MergeRule(MergeConfig<F, T> mergeConfig) {
        this.mergeConfig = mergeConfig;
    }

    /**
     * merge from node and to node with child merge result list.
     *
     * @param fromNode from node
     * @param toNode to node
     * @param childrenResultNode child merge result list
     * @return rel node
     */
    public abstract RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode);

    /**
     * copy input of RelNode with children in result node.
     *
     * @param relNode RelNode
     * @param childrenResultNode childrenResultNode
     * @return rel
     */
    protected final RelNode copy(RelNode relNode, ResultNodeList<RelNode> childrenResultNode) {
        if (childrenResultNode == null || childrenResultNode.isEmpty()) {
            return relNode;
        }
        if (relNode == null) {
            return null;
        }
        final List<RelNode> inputs = new ArrayList<>();
        for (ResultNode<RelNode> resultNode : childrenResultNode) {
            inputs.add(resultNode.getPayload());
        }
        return relNode.copy(relNode.getTraitSet(), inputs);
    }
}
