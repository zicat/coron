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

    protected final List<MergeConfig<?, ?>> mergeRuleConfigs;

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
    public ResultNode<RelNode> merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        for (MergeConfig<?, ?> config : mergeRuleConfigs) {
            if (match(config, fromNode, toNode)) {
                final MergeRule<?, ?> rule = config.toMergeRule();
                final RelNode relNode = rule.merge(fromNode, toNode, childrenResultNode);
                final ResultNode<RelNode> resultNode = ResultNode.of(relNode, childrenResultNode);
                resultNode.setFromLookAhead(lookAhead(config, TwoMergeType::fromRelNodeType));
                resultNode.setToLookAhead(lookAhead(config, TwoMergeType::toRelNodeType));
                return resultNode;
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
    private int lookAhead(TwoMergeType<?, ?> root, ConfigRelNodeTypeHandler handler) {
        int ahead = 0;
        final Queue<TwoMergeType<?, ?>> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            final TwoMergeType<?, ?> config = queue.poll();
            if (config.parent != null) {
                queue.offer(config.parent);
            }
            if (handler.getType(config) != RelNode.class) {
                ahead++;
            }
        }
        return ahead;
    }

    /**
     * match config with from node and to node.
     *
     * @param config config
     * @param fromNode fromNode
     * @param toNode toNode
     * @return boolean
     */
    private boolean match(TwoMergeType<?, ?> config, Node<RelNode> fromNode, Node<RelNode> toNode) {

        if (config.parent == null
                || match(config.parent, fromNode.getParent(), toNode.getParent())) {
            boolean fromTrue =
                    (config.fromRelNodeType == RelNode.class)
                            || (fromNode != null
                                    && config.fromRelNodeType.isInstance(fromNode.getPayload()));
            boolean toTrue =
                    (config.toRelNodeType == RelNode.class)
                            || (toNode != null
                                    && config.toRelNodeType.isInstance(toNode.getPayload()));
            return fromTrue && toTrue;
        }
        return false;
    }

    /** ConfigRelNodeTypeHandler. */
    private interface ConfigRelNodeTypeHandler {

        /**
         * get one type of TowMergeType.
         *
         * @param config TwoMergeType
         * @return Class
         */
        Class<?> getType(TwoMergeType<?, ?> config);
    }
}
