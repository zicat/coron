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
import org.zicat.coron.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** FilterMergeRule. */
public class FilterMergeRule extends MergeRule {

    final RexBuilder rexBuilder;

    public FilterMergeRule(Config mergeConfig) {
        super(mergeConfig);
        this.rexBuilder = new RexBuilder(mergeConfig.relDataTypeFactory);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        if (childrenResultNode.size() != 1) {
            return null;
        }

        final Filter fromFilter = (Filter) fromNode.getPayload();
        final Filter toFilter = (Filter) toNode.getPayload();
        final RelNode newInput = childrenResultNode.get(0).getPayload();
        if (mergeConfig.canMaterialized() && containsAggregate(newInput)) {
            return null;
        }

        final Map<String, Integer> fieldIndexMapping = dataTypeNameIndex(newInput.getRowType());
        final RexNode newFromCondition =
                createNewInputRexNode(
                        fromFilter.getCondition(), fromFilter.getInput(), fieldIndexMapping);
        final RexNode newToCondition =
                createNewInputRexNode(
                        toFilter.getCondition(), toFilter.getInput(), fieldIndexMapping);
        final List<RexNode> orList = Arrays.asList(newFromCondition, newToCondition);
        orList.sort((o1, o2) -> o2.toString().compareTo(o1.toString()));

        // sort condition to make output result uniqueness
        final RexNode newCondition = RexUtil.composeDisjunction(rexBuilder, orList);
        return fromFilter.copy(fromFilter.getTraitSet(), newInput, newCondition);
    }

    /** Filter Config. */
    public static class Config extends MergeConfig {

        protected RelDataTypeFactory relDataTypeFactory;

        public static Config create() {
            return new Config()
                    .relDataTypeFactory(CalciteContext.DEFAULT_SQL_TYPE_FACTORY)
                    .withOperandSupplier(Operand.of(Filter.class, Filter.class))
                    .as(Config.class);
        }

        public Config relDataTypeFactory(RelDataTypeFactory relDataTypeFactory) {
            this.relDataTypeFactory = relDataTypeFactory;
            return this;
        }

        @Override
        public FilterMergeRule toMergeRule() {
            return new FilterMergeRule(this);
        }
    }
}
