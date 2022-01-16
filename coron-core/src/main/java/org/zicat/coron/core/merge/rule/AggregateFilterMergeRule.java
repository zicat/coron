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
import org.zicat.coron.core.merge.MergeConfig;
import org.zicat.coron.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;

/** AggregateFilterMergeRule. */
public class AggregateFilterMergeRule extends MergeRule {

    public AggregateFilterMergeRule(MergeConfig mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        // calcite is not support materialized aggregate with filter(having)
        return null;
    }

    /** Config. */
    public static class Config extends MergeConfig {

        public static Config createFrom() {
            return new Config()
                    .withOperandSupplier(
                            Operand.ofFrom(Aggregate.class).operand(Operand.ofFrom(Filter.class)))
                    .as(AggregateFilterMergeRule.Config.class);
        }

        public static Config createTo() {
            return new Config()
                    .withOperandSupplier(
                            Operand.ofTo(Aggregate.class).operand(Operand.ofTo(Filter.class)))
                    .as(AggregateFilterMergeRule.Config.class);
        }

        @Override
        public AggregateFilterMergeRule toMergeRule() {
            return new AggregateFilterMergeRule(this);
        }
    }
}
