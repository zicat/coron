package io.agora.cruise.analyzer.shuttle;

import io.agora.cruise.core.rel.RelShuttleChainException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AggregateCallWrapper;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * PartitionAggregateFilterSimplify.
 *
 * <p>trans: select sum(x) where f(t) = 1 to select t,sum(x) group by f(t). (t in partitionFields)
 */
public class PartitionAggregateProjectRelShuttle extends PartitionRelShuttle {

    public PartitionAggregateProjectRelShuttle(List<String> partitionFields) {
        super(partitionFields);
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {

        final RelNode newNode = super.visit(aggregate);
        if (!(newNode instanceof Aggregate)
                || !(((Aggregate) newNode).getInput() instanceof Project)) {
            return newNode;
        }

        final Aggregate newAggregate = (Aggregate) newNode;

        // if aggregation function rollup is null, return
        for (AggregateCall aggregateCall : newAggregate.getAggCallList()) {
            SqlAggFunction sqlAggFunction = aggregateCall.getAggregation();
            if (sqlAggFunction.getRollup() == null) {
                throw new RelShuttleChainException(
                        sqlAggFunction.getName() + " function not support rollup");
            }
        }

        // not support grouping set
        if (newAggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            throw new RelShuttleChainException("grouping setting not support");
        }

        for (String name : newNode.getRowType().getFieldNames()) {
            if (name.startsWith(getPrefixName())) {
                return newNode;
            }
        }

        final Project project = (Project) newAggregate.getInput();
        final List<Integer> originGroupSet = newAggregate.getGroupSet().asList();
        final List<Integer> newGroupSet = new ArrayList<>(originGroupSet);
        final List<String> projectNames =
                new ArrayList<>(newAggregate.getRowType().getFieldNames());

        for (int i = 0; i < project.getRowType().getFieldList().size(); i++) {
            RelDataTypeField field = project.getRowType().getFieldList().get(i);
            if (field.getName().startsWith(getPrefixName())) {
                projectNames.add(field.getName());
                newGroupSet.add(field.getIndex());
            }
        }
        if (newGroupSet.size() == originGroupSet.size()) {
            return newNode;
        }

        final Aggregate copyAggregate = newAggregate(newAggregate, newGroupSet);
        final List<RexNode> projectNodes = new ArrayList<>();
        for (String name : projectNames) {
            projectNodes.add(
                    rexBuilder.makeInputRef(
                            copyAggregate, findIdByName(copyAggregate.getRowType(), name)));
        }
        return LogicalProject.create(copyAggregate, new ArrayList<>(), projectNodes, projectNames);
    }

    /**
     * new Aggregate.
     *
     * @param aggregate aggregate
     * @param newGroupSet newGroupSet
     * @return Aggregate
     */
    private Aggregate newAggregate(Aggregate aggregate, List<Integer> newGroupSet) {

        if (aggregate.getGroupCount() != 0) {
            return aggregate.copy(
                    aggregate.getTraitSet(),
                    aggregate.getInput(),
                    ImmutableBitSet.of(newGroupSet),
                    null,
                    aggregate.getAggCallList());
        }

        // if origin group set is empty, calcite will return the type cast as nullable, cause check
        // fail.
        List<AggregateCall> newCalls = new ArrayList<>();
        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            try {
                newCalls.add(AggregateCallWrapper.createWrapper(aggregateCall));
            } catch (Exception e) {
                throw new RelShuttleChainException("create new function error", e);
            }
        }

        return aggregate.copy(
                aggregate.getTraitSet(),
                aggregate.getInput(),
                ImmutableBitSet.of(newGroupSet),
                null,
                newCalls);
    }

    /**
     * find name by relDataType.
     *
     * @param relDataType relDataType
     * @param name name
     * @return id
     */
    private int findIdByName(RelDataType relDataType, String name) {
        int tmpSize = 0;
        int index =
                name.startsWith("$f") ? Integer.parseInt(name.replace("$f", "").split("_")[0]) : -1;
        for (int i = 0; i < relDataType.getFieldNames().size(); i++) {
            String field = relDataType.getFieldNames().get(i);
            if (field.startsWith(getPrefixName())) {
                tmpSize++;
            }
            if (index != -1 && field.equals("$f" + (index + tmpSize))) {
                return i;
            }
            if (index == -1 && field.equals(name)) {
                return i;
            }
        }
        throw new RuntimeException("not fount name " + name);
    }
}
