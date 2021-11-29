package io.agora.cruise.core.merge;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.rel.RelNode;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
    public ResultNode<RelNode> merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        for (MergeConfig<?, ?> config : mergeRuleConfigs) {
            if (match(config, fromNode, toNode)) {
                RelNode relNode = config.toMergeRule().merge(fromNode, toNode, childrenResultNode);
                ResultNode<RelNode> resultNode = ResultNode.of(relNode, childrenResultNode);
                resultNode.setThisLookAHead(lookAhead(config, true));
                resultNode.setOtherLookAhead(lookAhead(config, false));
                return resultNode;
            }
        }
        return ResultNode.of(null, childrenResultNode);
    }

    private int lookAhead(TwoMergeType<?, ?> root, boolean from) {
        int ahead = 0;
        final Queue<TwoMergeType<?, ?>> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            TwoMergeType<?, ?> config = queue.poll();
            if (config.parent != null) {
                queue.offer(config.parent);
            }
            if (from && config.fromRelNodeType != RelNode.class) {
                ahead++;
            }
            if (!from && config.toRelNodeType != RelNode.class) {
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
}
