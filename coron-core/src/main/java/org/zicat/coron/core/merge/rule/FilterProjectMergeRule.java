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

import org.zicat.coron.core.Node;
import org.zicat.coron.core.ResultNodeList;
import org.zicat.coron.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;

/** FilterProjectMergeRule. */
public class FilterProjectMergeRule extends ProjectMergeRule {

    public FilterProjectMergeRule(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        return super.merge(fromNode.getParent(), toNode, childrenResultNode);
    }

    /** FilterProjectMergeRule Config. */
    public static class Config extends ProjectMergeRule.Config {

        public static Config create() {
            return new Config()
                    .withOperandSupplier(
                            Operand.of(Filter.class, Project.class)
                                    .operand(Operand.ofFrom(Project.class)))
                    .as(Config.class);
        }

        @Override
        public FilterProjectMergeRule toMergeRule() {
            return new FilterProjectMergeRule(this);
        }
    }
}
