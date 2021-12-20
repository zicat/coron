package io.agora.cruise.analyzer.simplify;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.analyzer.util.Lists;
import io.agora.cruise.core.util.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

        aggregate = (LogicalAggregate) super.visit(aggregate);

        final Tuple2<Project, Filter> childTuple = parserLegal(aggregate);
        if (childTuple == null) {
            return super.visit(aggregate);
        }
        final Project project = childTuple.f0;
        final Filter filter = childTuple.f1;
        final Tuple2<RelNode, List<RexNode>> tuple = transFilterCondition(filter);
        if (tuple == null) {
            return super.visit(aggregate);
        }
        final List<RelDataTypeField> originalFields =
                project == null
                        ? filter.getRowType().getFieldList()
                        : project.getRowType().getFieldList();
        final List<RelDataTypeField> newRelTypeFields = new ArrayList<>(originalFields);
        final List<Integer> newGroupSet = new ArrayList<>(aggregate.getGroupSet().asList());
        final List<String> allProjectName = new ArrayList<>(aggregate.getRowType().getFieldNames());

        for (int i = 0; i < tuple.f1.size(); i++) {
            RexNode rexNode = tuple.f1.get(i);
            int index = i + newRelTypeFields.size();
            String name = getPrefixName() + index;
            newRelTypeFields.add(new RelDataTypeFieldImpl(name, index, rexNode.getType()));
            allProjectName.add(name);
            newGroupSet.add(index);
        }
        final List<RexNode> newExps = new ArrayList<>();
        if (project == null) {
            for (int i = 0; i < originalFields.size(); i++) {
                newExps.add(rexBuilder.makeInputRef(filter, i));
            }
        }
        final RelNode newAggregateInput =
                project != null
                        ? project.copy(
                                project.getTraitSet(),
                                tuple.f0,
                                Lists.merge(project.getProjects(), tuple.f1),
                                new RelRecordType(newRelTypeFields))
                        : LogicalProject.create(
                                tuple.f0,
                                new ArrayList<>(),
                                Lists.merge(newExps, tuple.f1),
                                new RelRecordType(newRelTypeFields));
        final Aggregate newAggregate =
                aggregate.copy(
                        aggregate.getTraitSet(),
                        newAggregateInput,
                        ImmutableBitSet.of(newGroupSet),
                        null,
                        aggregate.getAggCallList());

        final List<RexNode> allProjectNodes =
                allProjectName.stream()
                        .map(
                                name ->
                                        rexBuilder.makeInputRef(
                                                newAggregate,
                                                findIdByName(newAggregate.getRowType(), name)))
                        .collect(Collectors.toList());
        return LogicalProject.create(
                newAggregate, new ArrayList<>(), allProjectNodes, allProjectName);
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
        int index = name.startsWith("$f") ? Integer.parseInt(name.replace("$f", "")) : -1;
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

    /**
     * parser legal aggregate.
     *
     * @param aggregate aggregate
     * @return tuple of child project and subChild filter.
     */
    private Tuple2<Project, Filter> parserLegal(Aggregate aggregate) {

        if (aggregate.getInput() instanceof Filter) {
            return Tuple2.of(null, (Filter) aggregate.getInput());
        }

        if (!(aggregate.getInput() instanceof Project)
                || !aggregate.getGroupSets().equals(ImmutableList.of(aggregate.getGroupSet()))) {
            return null;
        }

        final Project project = (Project) aggregate.getInput();
        if (project.getInput() == null || !(project.getInput() instanceof Filter)) {
            return null;
        }

        final Filter filter = (Filter) project.getInput();
        return Tuple2.of(project, filter);
    }
}
