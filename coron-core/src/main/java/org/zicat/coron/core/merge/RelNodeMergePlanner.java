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

package org.zicat.coron.core.merge;

import org.apache.calcite.rel.RelNode;
import org.zicat.coron.core.Node;
import org.zicat.coron.core.ResultNode;
import org.zicat.coron.core.ResultNodeList;
import org.zicat.coron.core.merge.rule.MergeRule;

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
