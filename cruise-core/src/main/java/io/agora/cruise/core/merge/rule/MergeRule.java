package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

/** MergeRule. */
public abstract class MergeRule {

    protected final MergeConfig mergeConfig;

    public MergeRule(MergeConfig mergeConfig) {
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
     * create new RexNode that inputRef replace from fromInput to newInput.
     *
     * @param rexNode rexNode
     * @param originalInput originalInput
     * @param newInput newInput
     * @return RexNode
     */
    protected RexNode createNewInputRexNode(
            RexNode rexNode, RelNode originalInput, RelNode newInput) {
        return rexNode.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int index = inputRef.getIndex();
                        String name = originalInput.getRowType().getFieldNames().get(index);
                        int newIndex = findIndexByName(newInput.getRowType(), name);
                        return newIndex == -1
                                ? inputRef
                                : new RexInputRef(newIndex, inputRef.getType());
                    }
                });
    }

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

    /**
     * find index by name from RelDataType.
     *
     * @param relDataType relDataType
     * @param name name
     * @return -1 if not found else return position
     */
    protected final int findIndexByName(RelDataType relDataType, String name) {
        for (int i = 0; i < relDataType.getFieldNames().size(); i++) {
            if (relDataType.getFieldNames().get(i).equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
