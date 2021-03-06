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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** AggregateMergeableCheck. */
public class AggregateMergeableCheck extends RelShuttleImpl {

    private Set<String> groupFields = null;
    private Set<String> projectFields = null;
    private final AtomicBoolean mergeable = new AtomicBoolean(true);

    /**
     * check rel node mergeable.
     *
     * @param relNode relNode
     * @return boolean mergeable
     */
    public static boolean mergeable(RelNode relNode) {
        AggregateMergeableCheck check = new AggregateMergeableCheck();
        relNode.accept(check);
        return check.mergeable.get();
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        filter.getCondition().accept(new RexShuttleCheck(filter.getInput().getRowType()));
        groupFields = null;
        projectFields = null;
        return super.visit(filter);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        projectFields = new HashSet<>();
        for (RexNode rexNode : project.getProjects()) {
            RexNode expectRexNode = rexNode;
            if (rexNode.getKind() == SqlKind.AS) {
                RexCall asCall = (RexCall) rexNode;
                expectRexNode = asCall.getOperands().get(0);
            }
            if (!(expectRexNode instanceof RexInputRef)) {
                continue;
            }
            RexInputRef inputRef = (RexInputRef) expectRexNode;
            String newField = getFieldById(project.getInput(), inputRef.getIndex());
            projectFields.add(newField);
        }

        return super.visit(project);
    }

    /**
     * get name by index.
     *
     * @param relNode rel node
     * @param index index
     * @return name
     */
    private static String getFieldById(RelNode relNode, int index) {
        return relNode.getRowType().getFieldNames().get(index);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {

        groupFields = new HashSet<>();
        if (aggregate.getGroupSets().size() > 1) {
            for (ImmutableBitSet bitSet : aggregate.getGroupSets()) {
                if (bitSet.size() != 1) {
                    continue;
                }
                Integer id = bitSet.toList().get(0);
                groupFields.add(getFieldById(aggregate.getInput(), id));
            }
        } else {
            List<Integer> groupSet = aggregate.getGroupSets().get(0).toList();
            groupSet.forEach(id -> groupFields.add(getFieldById(aggregate.getInput(), id)));
        }
        return super.visit(aggregate);
    }

    /** RexShuttleCheck. */
    private class RexShuttleCheck extends RexShuttle {

        private final RelDataType relDataType;

        public RexShuttleCheck(RelDataType relDataType) {
            this.relDataType = relDataType;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            String name = relDataType.getFieldNames().get(inputRef.getIndex());
            if (groupFields == null
                    || projectFields == null
                    || !groupFields.contains(name)
                    || !projectFields.contains(name)) {
                mergeable.set(false);
            }
            return super.visitInputRef(inputRef);
        }
    }
}
