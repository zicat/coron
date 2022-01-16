/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.core.merge.rule;

import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.zicat.coron.core.Node;
import org.zicat.coron.core.ResultNode;
import org.zicat.coron.core.ResultNodeList;
import org.zicat.coron.core.merge.MergeConfig;
import org.zicat.coron.parser.util.CoronParserException;

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
                            throw new CoronParserException(
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
