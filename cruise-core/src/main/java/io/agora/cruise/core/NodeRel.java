package io.agora.cruise.core;

import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.RelNodeMergePlanner;
import io.agora.cruise.core.merge.rule.*;
import org.apache.calcite.rel.RelNode;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/** NodeRel. */
public class NodeRel extends Node<RelNode> {

    protected final RelNodeMergePlanner mergePlanner;

    protected NodeRel(RelNodeMergePlanner mergePlanner, Node<RelNode> parent, RelNode payload) {
        super(parent, payload);
        this.mergePlanner = mergePlanner;
    }

    protected NodeRel(RelNodeMergePlanner mergePlanner, RelNode payload) {
        this(mergePlanner, null, payload);
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

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot, RelNodeMergePlanner mergePlanner) {
        final Queue<NodeRel> queue = new LinkedList<>();
        final NodeRel nodeRoot = new NodeRel(mergePlanner, relRoot);
        queue.offer(nodeRoot);
        while (!queue.isEmpty()) {
            Node<RelNode> node = queue.poll();
            RelNode relNode = node.getPayload();
            for (int i = 0; i < relNode.getInputs().size(); i++) {
                queue.offer(new NodeRel(mergePlanner, node, relNode.getInput(i)));
            }
        }
        return nodeRoot;
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot) {
        List<MergeConfig<?, ?>> mergeRuleConfigs =
                Arrays.asList(
                        TableScanMergeRule.Config.DEFAULT,
                        ProjectMergeRule.Config.DEFAULT,
                        FilterMergeRule.Config.DEFAULT,
                        AggregateMergeRule.Config.DEFAULT,
                        JoinMergeRule.Config.DEFAULT,
                        FilterProjectMerge.Config.DEFAULT,
                        ProjectFilterMerge.Config.DEFAULT);

        RelNodeMergePlanner mergePlanner = new RelNodeMergePlanner(mergeRuleConfigs);
        return createNodeRelRoot(relRoot, mergePlanner);
    }
}
