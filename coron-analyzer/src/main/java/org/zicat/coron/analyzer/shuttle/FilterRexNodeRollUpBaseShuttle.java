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

package org.zicat.coron.analyzer.shuttle;

import org.apache.calcite.rel.PredictRexShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Pair;
import org.zicat.coron.analyzer.rel.RelShuttleChainException;
import org.zicat.coron.parser.CalciteContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** PartitionSimplify. */
public abstract class FilterRexNodeRollUpBaseShuttle extends RelShuttleImpl {

    private static final List<SqlKind> COMPARE_KIND =
            Arrays.asList(
                    SqlKind.EQUALS,
                    SqlKind.BETWEEN,
                    SqlKind.LESS_THAN,
                    SqlKind.LESS_THAN_OR_EQUAL,
                    SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.SEARCH,
                    SqlKind.IN);

    protected final RexBuilder rexBuilder = defaultRexBuilder();
    protected final String rollUpField;

    protected FilterRexNodeRollUpBaseShuttle(String rollUpField) {
        this.rollUpField = rollUpField;
    }

    /**
     * create defaultRexBuilder.
     *
     * @return RexBuilder
     */
    protected RexBuilder defaultRexBuilder() {
        return new RexBuilder(defaultTypeFactory());
    }

    /**
     * create defaultTypeFactory.
     *
     * @return SqlTypeFactoryImpl
     */
    protected SqlTypeFactoryImpl defaultTypeFactory() {
        return CalciteContext.DEFAULT_SQL_TYPE_FACTORY;
    }

    /**
     * create newId by newInput row type.
     *
     * @param id id
     * @param newInput newInput
     * @return newId
     */
    protected int findNewId(int id, RelNode newInput) {
        return findNewId(id, newInput.getRowType().getFieldNames());
    }

    /**
     * create newId by newNames.
     *
     * @param id id
     * @param fieldList fieldList
     * @return newId
     */
    protected int findNewId(int id, List<String> fieldList) {
        int idOffset = id;
        int start = 0;
        while (start <= idOffset) {
            String name = fieldList.get(start);
            if (name.startsWith(getPrefixName())) {
                idOffset++;
            }
            start++;
        }
        return idOffset;
    }

    /**
     * get prefix name,default tmp_p_ .
     *
     * @return prefix name
     */
    protected String getPrefixName() {
        return rollUpField + "_roll_up_";
    }

    /**
     * get filter rex node.
     *
     * @param filter filter
     * @return tuple2
     */
    protected Pair<RelNode, List<RexNode>> transFilterCondition(Filter filter, RelNode input) {

        // if input already has prefixName field, this filterNode skip check.
        for (String name : input.getRowType().getFieldNames()) {
            if (name.startsWith(getPrefixName())) {
                return null;
            }
        }

        final RexNode rexNode = filter.getCondition();
        final RexNode firstOperand = compareCallFirstOperand(rexNode, input);
        if (firstOperand != null) {
            return Pair.of(input, Collections.singletonList(firstOperand));
        }
        if (rexNode.getKind() != SqlKind.AND) {
            return null;
        }
        final RexCall andRexCall = (RexCall) rexNode;
        final List<RexNode> noPartitionRexNode = new ArrayList<>();
        final List<RexNode> partitionRexNode = new ArrayList<>();
        for (int i = 0; i < andRexCall.getOperands().size(); i++) {
            final RexNode childNode = andRexCall.getOperands().get(i);
            final RexNode childFirstOperand =
                    compareCallFirstOperand(andRexCall.getOperands().get(i), input);
            if (childFirstOperand == null) {
                noPartitionRexNode.add(childNode);
            } else if (!partitionRexNode.contains(childFirstOperand)) {
                partitionRexNode.add(childFirstOperand);
            }
        }
        if (partitionRexNode.isEmpty()) {
            return null;
        }
        if (partitionRexNode.size() > 1) {
            throw new RelShuttleChainException("only support transfer one type filter");
        }
        if (noPartitionRexNode.isEmpty()) {
            return Pair.of(input, partitionRexNode);
        }
        final RexNode newCondition =
                noPartitionRexNode.size() == 1
                        ? noPartitionRexNode.get(0)
                        : andRexCall.clone(andRexCall.type, noPartitionRexNode);
        return Pair.of(filter.copy(filter.getTraitSet(), input, newCondition), partitionRexNode);
    }

    /**
     * containsPartitionField. like function(a) > 10 , return function(a)
     *
     * @param rexNode rexNode
     * @param input filter
     * @return boolean
     */
    private RexNode compareCallFirstOperand(RexNode rexNode, RelNode input) {

        if (rexNode.getKind() == SqlKind.OR) {
            final RexCall orCall = (RexCall) rexNode;
            RexNode result = null;
            for (RexNode operand : orCall.getOperands()) {
                RexNode firstOperand = compareCallFirstOperand(operand, input);
                if (firstOperand == null) {
                    return null;
                }
                if (result == null || result.equals(firstOperand)) {
                    result = firstOperand;
                }
            }
            return result;
        }

        if (!containsCompareKind(rexNode)) {
            return null;
        }
        final RexCall functionCall = (RexCall) rexNode;
        final RexNode firstOperand = functionCall.getOperands().get(0);
        final RelDataType relDataType = input.getRowType();
        for (int i = 1; i < functionCall.getOperands().size(); i++) {
            final RexNode rightRexNode = functionCall.getOperands().get(i);
            if (!PredictRexShuttle.predicts(rightRexNode).isEmpty()) {
                return null;
            }
        }
        return PartitionFieldFounder.contains(firstOperand, relDataType, rollUpField);
    }

    /**
     * check rexNode kind in compare kind.
     *
     * @param rexNode rexNode
     * @return boolean contains
     */
    protected boolean containsCompareKind(RexNode rexNode) {
        return COMPARE_KIND.contains(rexNode.getKind());
    }

    /**
     * create newCondition mapping with newInput.
     *
     * @param rexNode rexNode
     * @param newInput newInput
     * @return RexNode
     */
    protected RexNode newCondition(RexNode rexNode, RelNode newInput) {
        return newCondition(rexNode, newInput.getRowType().getFieldNames());
    }

    /**
     * create newCondition mapping with newFieldList.
     *
     * @param rexNode rexNode
     * @param fieldList newInput
     * @return RexNode
     */
    protected RexNode newCondition(RexNode rexNode, List<String> fieldList) {
        return rexNode.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int newId = findNewId(inputRef.getIndex(), fieldList);
                        return new RexInputRef(newId, inputRef.getType());
                    }
                });
    }

    /** PartitionFieldFounder. */
    private static class PartitionFieldFounder extends RexShuttle {

        private final RelDataType relDataType;
        private final String rollUpField;
        private final AtomicBoolean contains = new AtomicBoolean(false);

        public PartitionFieldFounder(RelDataType relDataType, String rollUpField) {
            this.relDataType = relDataType;
            this.rollUpField = rollUpField;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (rollUpField.equals(relDataType.getFieldNames().get(inputRef.getIndex()))) {
                contains.set(true);
            }
            return inputRef;
        }

        /**
         * check rexNode contains partition fields.
         *
         * @param rexNode reNode
         * @param relDataType relDataType
         * @param rollUpField rollUpField
         * @return rexNode if contains else null
         */
        public static RexNode contains(
                RexNode rexNode, RelDataType relDataType, String rollUpField) {

            final PartitionFieldFounder fieldFounder =
                    new PartitionFieldFounder(relDataType, rollUpField);
            rexNode.accept(fieldFounder);
            return fieldFounder.contains.get() ? rexNode : null;
        }
    }
}
