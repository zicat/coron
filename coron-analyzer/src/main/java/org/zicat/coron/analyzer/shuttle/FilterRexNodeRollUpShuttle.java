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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AggregateCallWrapper;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.zicat.coron.analyzer.rel.RelShuttleChainException;
import org.zicat.coron.analyzer.util.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * PartitionAggregateFilterSimplify.
 *
 * <p>trans: select sum(x) where f(t) = 1 to select t,sum(x) group by f(t). (t in partitionFields)
 */
public class FilterRexNodeRollUpShuttle extends FilterRexNodeRollUpBaseShuttle {

    public FilterRexNodeRollUpShuttle(String rollUpField) {
        super(rollUpField);
    }

    /**
     * create partition shuttles combo.
     *
     * @param rollUpField rollUpField
     * @return RelShuttleImpl[]
     */
    public static RelShuttleImpl[] partitionShuttles(String rollUpField) {
        return new RelShuttleImpl[] {new FilterRexNodeRollUpShuttle(rollUpField)};
    }

    @Override
    public RelNode visit(LogicalFilter filter) {

        final RelNode newInput = filter.getInput().accept(this);
        final Pair<RelNode, List<RexNode>> tuple = transFilterCondition(filter, newInput);
        if (tuple == null) {
            final RexNode newCondition = newCondition(filter.getCondition(), newInput);
            return filter.copy(filter.getTraitSet(), newInput, newCondition);
        }

        final List<RelDataTypeField> newRelTypeFields =
                new ArrayList<>(filter.getRowType().getFieldList());
        final List<RexNode> newProjects = new ArrayList<>();
        for (int i = 0; i < newRelTypeFields.size(); i++) {
            newProjects.add(new RexInputRef(i, newRelTypeFields.get(i).getType()));
        }

        for (int i = 0; i < tuple.right.size(); i++) {
            final RexNode rexNode = tuple.right.get(i);
            final int index = i + newRelTypeFields.size();
            final String name = getPrefixName() + index;
            newRelTypeFields.add(new RelDataTypeFieldImpl(name, index, rexNode.getType()));
        }
        return LogicalProject.create(
                tuple.left,
                new ArrayList<>(),
                Lists.merge(newProjects, tuple.right),
                new RelRecordType(newRelTypeFields));
    }

    @Override
    public RelNode visit(LogicalProject project) {

        final RelNode newInput = project.getInput().accept(this);
        final List<RelDataTypeField> inputNames = newInput.getRowType().getFieldList();
        final List<Pair<RexNode, String>> prefixProjects = getPrefixRexNode(inputNames);
        final List<RexNode> newProjects = new ArrayList<>();
        for (RexNode function : project.getProjects()) {
            newProjects.add(newCondition(function, newInput));
        }

        final List<RelDataTypeField> newRelTypeFields =
                new ArrayList<>(project.getRowType().getFieldList());
        for (int i = 0; i < prefixProjects.size(); i++) {
            Pair<RexNode, String> tuple2 = prefixProjects.get(i);
            final RexNode rexNode = tuple2.left;
            final int index = i + newRelTypeFields.size();
            newRelTypeFields.add(new RelDataTypeFieldImpl(tuple2.right, index, rexNode.getType()));
            newProjects.add(tuple2.left);
        }
        return LogicalProject.create(
                newInput, project.getHints(), newProjects, new RelRecordType(newRelTypeFields));
    }

    /**
     * found name start with prefix name.
     *
     * @param inputNames inputNames
     * @return boolean contains
     */
    private List<Pair<RexNode, String>> getPrefixRexNode(List<RelDataTypeField> inputNames) {
        final List<Pair<RexNode, String>> ids = new ArrayList<>();
        for (int i = 0; i < inputNames.size(); i++) {
            RelDataTypeField field = inputNames.get(i);
            if (field.getName().startsWith(getPrefixName())) {
                ids.add(Pair.of(new RexInputRef(i, field.getType()), field.getName()));
            }
        }
        return ids;
    }

    @Override
    public RelNode visit(LogicalJoin join) {

        final RelNode newLeft = join.getLeft().accept(this);
        final RelNode newRight = join.getRight().accept(this);
        final RexNode condition = join.getCondition();
        final List<String> allNames =
                Lists.merge(
                        newLeft.getRowType().getFieldNames(),
                        newRight.getRowType().getFieldNames());
        final RexNode newIndexCondition = newCondition(condition, allNames);
        final RexNode leftRexNode = getFirstOnePrefixId(newLeft, 0);
        final RexNode rightRexNode =
                getFirstOnePrefixId(newRight, newLeft.getRowType().getFieldCount());
        RexNode andNode = null;
        if (leftRexNode != null && rightRexNode != null) {
            andNode =
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS, Arrays.asList(leftRexNode, rightRexNode));
        }
        final RexNode newCondition =
                andNode == null
                        ? newIndexCondition
                        : rexBuilder.makeCall(
                                SqlStdOperatorTable.AND, Arrays.asList(newIndexCondition, andNode));
        return join.copy(
                join.getTraitSet(),
                newCondition,
                newLeft,
                newRight,
                join.getJoinType(),
                join.isSemiJoinDone());
    }

    /**
     * get getFirstOnePrefixId by relNode.
     *
     * @param relNode relNode
     * @return id
     */
    private RexNode getFirstOnePrefixId(RelNode relNode, int offset) {
        for (int i = 0; i < relNode.getRowType().getFieldList().size(); i++) {
            final RelDataTypeField field = relNode.getRowType().getFieldList().get(i);
            if (field.getName().startsWith(getPrefixName())) {
                return new RexInputRef(field.getIndex() + offset, field.getType());
            }
        }
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {

        final RelNode newInput = aggregate.getInput().accept(this);
        final List<Integer> originGroupSet = aggregate.getGroupSet().asList();
        final List<Integer> newOriginGroupSet = new ArrayList<>();
        for (Integer groupId : originGroupSet) {
            newOriginGroupSet.add(findNewId(groupId, newInput));
        }
        final List<Integer> prefixGroupSet = new ArrayList<>();
        for (int i = 0; i < newInput.getRowType().getFieldList().size(); i++) {
            final RelDataTypeField field = newInput.getRowType().getFieldList().get(i);
            if (field.getName().startsWith(getPrefixName())) {
                prefixGroupSet.add(i);
            }
        }
        if (prefixGroupSet.isEmpty()) {
            return aggregate.copy(
                    aggregate.getTraitSet(),
                    newInput,
                    aggregate.getGroupSet(),
                    null,
                    aggregate.getAggCallList());
        }

        // if aggregation function rollup is null, return
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            final SqlAggFunction sqlAggFunction = aggregateCall.getAggregation();
            if (sqlAggFunction.getRollup() == null) {
                throw new RelShuttleChainException(
                        sqlAggFunction.getName() + " function not support rollup");
            }
        }

        // not support grouping set
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            throw new RelShuttleChainException("grouping setting not support");
        }
        final List<Integer> newGroupSet =
                Lists.merge(newOriginGroupSet, prefixGroupSet).stream()
                        .distinct()
                        .collect(Collectors.toList());
        final List<AggregateCall> newCalls = new ArrayList<>();
        for (AggregateCall call : aggregate.getAggCallList()) {
            try {
                final List<Integer> newArgList =
                        call.getArgList().stream()
                                .map(id -> findNewId(id, newInput))
                                .collect(Collectors.toList());
                if (aggregate.getGroupCount() != 0) {
                    newCalls.add(
                            AggregateCall.create(
                                    call.getAggregation(),
                                    call.isDistinct(),
                                    call.isApproximate(),
                                    call.ignoreNulls(),
                                    newArgList,
                                    call.filterArg,
                                    call.distinctKeys,
                                    call.collation,
                                    call.type,
                                    call.name));
                } else {
                    newCalls.add(AggregateCallWrapper.createWrapper(call, newArgList));
                }
            } catch (Exception e) {
                throw new RelShuttleChainException("create new function error", e);
            }
        }
        return aggregate.copy(
                aggregate.getTraitSet(), newInput, ImmutableBitSet.of(newGroupSet), null, newCalls);
    }
}
