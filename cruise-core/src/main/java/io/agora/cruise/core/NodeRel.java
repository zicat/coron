package io.agora.cruise.core;

import org.apache.calcite.rel.RelNode;

import java.util.LinkedList;
import java.util.Queue;
import java.util.stream.Collectors;

/** NodeRel. */
public class NodeRel extends Node<RelNode> {

    final RelNodeMergeable mergeable = new RelNodeMergeable();

    protected NodeRel(Node<RelNode> parent, RelNode payload) {
        super(parent, payload);
    }

    protected NodeRel(RelNode payload) {
        super(payload);
    }

    /**
     * boolean is similar.
     *
     * @param otherNode other node
     * @return boolean
     */
    @Override
    public ResultNode<RelNode> merge(
            Node<RelNode> otherNode, ResultNodeList<RelNode> childrenResultNode) {

        if (otherNode == null) {
            return ResultNode.of(childrenResultNode);
        }
        RelNode newPayload = mergeable.merge(this, otherNode);
        if (newPayload == null) {
            return ResultNode.of(childrenResultNode);
        }

        if (childrenResultNode == null) {
            return ResultNode.of(newPayload);
        }

        return ResultNode.of(
                newPayload.copy(
                        newPayload.getTraitSet(),
                        childrenResultNode.stream()
                                .map(v -> v.payload)
                                .collect(Collectors.toList())),
                childrenResultNode);
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot) {
        Queue<NodeRel> queue = new LinkedList<>();
        NodeRel nodeRoot = new NodeRel(relRoot);
        queue.offer(nodeRoot);
        while (!queue.isEmpty()) {
            Node<RelNode> node = queue.poll();
            RelNode relNode = node.getPayload();
            for (int i = 0; i < relNode.getInputs().size(); i++) {
                queue.offer(new NodeRel(node, relNode.getInput(i)));
            }
        }
        return nodeRoot;
    }
}
