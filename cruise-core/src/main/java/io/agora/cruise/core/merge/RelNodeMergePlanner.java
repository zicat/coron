package io.agora.cruise.core.merge;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/** RelNodeSimilar. */
public class RelNodeMergePlanner {

    protected List<MergeConfig<?, ?>> mergeRuleConfigs;

    public RelNodeMergePlanner(List<MergeConfig<?, ?>> mergeRuleConfigs) {
        this.mergeRuleConfigs = mergeRuleConfigs;
    }

    /**
     * merge from node and to node with children inputs.
     *
     * @param fromNode from node
     * @param toNode to node
     * @param childrenResultNode children inputs
     * @return new rel node
     */
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        final RelNode from = fromNode.getPayload();
        final RelNode to = toNode.getPayload();
        for (MergeConfig<?, ?> config : mergeRuleConfigs) {
            if (config.fromRelNodeType.isInstance(from) && config.toRelNodeType.isInstance(to)) {
                return config.toMergeRule().merge(fromNode, toNode, childrenResultNode);
            }
        }
        return null;
    }
}
