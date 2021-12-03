package io.agora.cruise.core.merge.rule;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** AggregateMergeableCheck. */
public class AggregateMergeableCheck extends RelShuttleImpl {

    private Set<String> groupFields = null;
    private Set<String> projectFields = null;
    private AtomicBoolean contains = new AtomicBoolean(true);

    public static boolean contains(RelNode relNode) {
        AggregateMergeableCheck check = new AggregateMergeableCheck();
        relNode.accept(check);
        return check.contains.get();
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        filter.getCondition()
                .accept(
                        new RexShuttle() {
                            @Override
                            public RexNode visitInputRef(RexInputRef inputRef) {
                                String name =
                                        filter.getInput()
                                                .getRowType()
                                                .getFieldNames()
                                                .get(inputRef.getIndex());
                                if (groupFields == null
                                        || projectFields == null
                                        || !groupFields.contains(name)
                                        || !projectFields.contains(name)) {
                                    contains.set(false);
                                }
                                return super.visitInputRef(inputRef);
                            }
                        });
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
            projectFields.add(
                    project.getInput().getRowType().getFieldNames().get(inputRef.getIndex()));
        }

        return super.visit(project);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {

        groupFields = new HashSet<>();
        if (aggregate.getGroupSets().size() > 1) {
            for (ImmutableBitSet bitSet : aggregate.getGroupSets()) {
                if (bitSet.size() != 1) {
                    continue;
                }
                Integer groupId = bitSet.toList().get(0);
                groupFields.add(aggregate.getInput().getRowType().getFieldNames().get(groupId));
            }
        } else {
            aggregate
                    .getGroupSets()
                    .get(0)
                    .toList()
                    .forEach(
                            groupId -> {
                                groupFields.add(
                                        aggregate
                                                .getInput()
                                                .getRowType()
                                                .getFieldNames()
                                                .get(groupId));
                            });
        }
        return super.visit(aggregate);
    }
}
