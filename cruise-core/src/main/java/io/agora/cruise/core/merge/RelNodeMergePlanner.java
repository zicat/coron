package io.agora.cruise.core.merge;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.rule.MergeRule;
import org.apache.calcite.rel.RelNode;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
                rn.setFromLookAhead(lookAhead(config.operand(), Operand::isAnyFromNodeType));
                rn.setToLookAhead(lookAhead(config.operand(), Operand::isAnyToNodeType));
                return rn;
            }
        }
        return ResultNode.of(null, childrenResultNode);
    }

    /**
     * compute the look ahead size by TwoMergeType.
     *
     * @param root root TwoMergeType
     * @param handler type handler
     * @return look ahead size
     */
    private int lookAhead(Operand root, ConfigRelNodeTypeHandler handler) {
        int ahead = 0;
        final Queue<Operand> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            final Operand operand = queue.poll();
            if (operand.parent() != null) {
                queue.offer(operand.parent());
            }
            if (!handler.isAnyNodeType(operand)) {
                ahead++;
            }
        }
        return ahead;
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

    /** ConfigRelNodeTypeHandler. */
    private interface ConfigRelNodeTypeHandler {

        /**
         * is any node type of config.
         *
         * @param operand operand
         * @return true if is any node type
         */
        boolean isAnyNodeType(Operand operand);
    }
}
