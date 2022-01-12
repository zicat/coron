package io.agora.cruise.core.merge.rule;

import com.google.common.collect.Maps;
import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.parser.util.CruiseParserException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
     * check relNode contains aggregation.
     *
     * <p>calcite: not support materialized topNode not equal aggregation.
     *
     * @param relNode relNode
     * @return boolean contains aggregate
     */
    protected boolean containsAggregate(RelNode relNode) {
        return TopAggregationFinder.contains(relNode);
    }

    /**
     * create new RexNode that inputRef replace from fromInput to newInput.
     *
     * @param rexNode rexNode
     * @param originalInput originalInput
     * @param newIndexMapping use dataTypeNameIndex(newInput.getRowType()) to build
     * @return RexNode
     */
    protected RexNode createNewInputRexNode(
            RexNode rexNode, RelNode originalInput, Map<String, Integer> newIndexMapping) {
        return rexNode.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        final int index = inputRef.getIndex();
                        final String name = originalInput.getRowType().getFieldNames().get(index);
                        final Integer realIndex = newIndexMapping.get(name);
                        if (realIndex == null) {
                            throw new CruiseParserException(
                                    "create new input RexNode fail, node detail: "
                                            + rexNode.toString());
                        }
                        return new RexInputRef(realIndex, inputRef.getType());
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
        final List<RelNode> inputs = new ArrayList<>(childrenResultNode.size());
        for (ResultNode<RelNode> resultNode : childrenResultNode) {
            inputs.add(resultNode.getPayload());
        }
        return relNode.copy(relNode.getTraitSet(), inputs);
    }

    /**
     * build relDataType Index.
     *
     * @param relDataType relDataType
     * @return mapIndex
     */
    protected static Map<String, Integer> dataTypeNameIndex(RelDataType relDataType) {
        Map<String, Integer> mapping = Maps.newHashMapWithExpectedSize(relDataType.getFieldCount());
        for (int i = 0; i < relDataType.getFieldNames().size(); i++) {
            mapping.put(relDataType.getFieldNames().get(i), i);
        }
        return mapping;
    }
}
