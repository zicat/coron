package io.agora.cruise.core.merge;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.rule.MergeRule;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/** RelNodeMergePlanner. */
public class RelNodeMergePlanner {

    protected final List<MergeConfig> mergeRuleConfigs;

    public RelNodeMergePlanner(List<MergeConfig> mergeRuleConfigs) {
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
    public ResultNode<RelNode> merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        for (MergeConfig config : mergeRuleConfigs) {
            if (match(config.operand(), fromNode, toNode)) {
                final MergeRule rule = config.toMergeRule();
                final RelNode relNode = rule.merge(fromNode, toNode, childrenResultNode);
                final ResultNode<RelNode> rn = ResultNode.of(relNode, childrenResultNode);
                rn.setFromLookAhead(config.fromLookAhead);
                rn.setToLookAhead(config.toLookAhead);
                return rn;
            }
        }
        return ResultNode.of(null, childrenResultNode);
    }

    /**
     * match config with from node and to node.
     *
     * @param operand config
     * @param fromNode fromNode
     * @param toNode toNode
     * @return boolean
     */
    private boolean match(Operand operand, Node<RelNode> fromNode, Node<RelNode> toNode) {

        if (operand.parent() == null
                || match(operand.parent(), fromNode.getParent(), toNode.getParent())) {
            final boolean fromTrue =
                    (operand.isAnyFromNodeType())
                            || (fromNode != null
                                    && operand.fromRelNodeType().isInstance(fromNode.getPayload()));
            final boolean toTrue =
                    (operand.isAnyToNodeType())
                            || (toNode != null
                                    && operand.toRelNodeType().isInstance(toNode.getPayload()));
            return fromTrue && toTrue;
        }
        return false;
    }
}
