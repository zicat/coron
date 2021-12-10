package io.agora.cruise.presto.simplify;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.core.NodeRel;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.presto.util.Lists;
import org.apache.calcite.rel.PredictRexShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * PartitionAggregateFilterSimplify.
 *
 * <p>trans: select sum(x) where f(t) = 1 to select t,sum(x) group by f(t). (t in partitionFields)
 */
public class PartitionAggregateFilterSimplify extends RelShuttleImpl implements NodeRel.Simplify {

    private static final List<SqlKind> COMPARE_KIND =
            Arrays.asList(
                    SqlKind.EQUALS,
                    SqlKind.BETWEEN,
                    SqlKind.LESS_THAN,
                    SqlKind.LESS_THAN_OR_EQUAL,
                    SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.IN);
    private static final String PREFIX_NAME = "tmp_p_";

    final RexBuilder rexBuilder = new RexBuilder(CalciteContext.DEFAULT_SQL_TYPE_FACTORY);
    private final List<String> partitionFields;

    public PartitionAggregateFilterSimplify(List<String> partitionFields) {
        this.partitionFields = partitionFields;
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

    @Override
    public RelNode apply(RelNode relNode) {
        if (partitionFields == null || partitionFields.isEmpty()) {
            return relNode;
        }
        return relNode.accept(this);
    }

    protected String getPrefixName() {
        return PREFIX_NAME;
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

    /**
     * get filter rex node.
     *
     * @param filter filter
     * @return tuple2
     */
    private Tuple2<RelNode, List<RexNode>> transFilterCondition(Filter filter) {

        final RexNode rexNode = filter.getCondition();
        final RexNode leftExp = leftExpParser(rexNode, filter);
        if (leftExp != null) {
            return new Tuple2<>(filter.getInput(), Collections.singletonList(leftExp));
        }
        if (rexNode.getKind() != SqlKind.AND) {
            return null;
        }
        final RexCall andRexCall = (RexCall) rexNode;
        final List<RexNode> noPartitionRexNode = new ArrayList<>();
        final List<RexNode> partitionRexNode = new ArrayList<>();
        for (int i = 0; i < andRexCall.getOperands().size(); i++) {
            final RexNode childNode = andRexCall.getOperands().get(i);
            final RexNode childLeftExp = leftExpParser(andRexCall.getOperands().get(i), filter);
            if (childLeftExp == null) {
                noPartitionRexNode.add(childNode);
            } else if (!partitionRexNode.contains(childLeftExp)) {
                partitionRexNode.add(childLeftExp);
            }
        }
        if (partitionRexNode.isEmpty()) {
            return null;
        }
        if (noPartitionRexNode.isEmpty()) {
            return new Tuple2<>(filter.getInput(), partitionRexNode);
        }
        final RexNode newCondition =
                noPartitionRexNode.size() == 1
                        ? noPartitionRexNode.get(0)
                        : andRexCall.clone(andRexCall.type, noPartitionRexNode);
        return new Tuple2<>(
                filter.copy(filter.getTraitSet(), filter.getInput(), newCondition),
                partitionRexNode);
    }

    /**
     * containsPartitionField.
     *
     * @param rexNode rexNode
     * @param filter filter
     * @return boolean
     */
    private RexNode leftExpParser(RexNode rexNode, Filter filter) {

        if (!COMPARE_KIND.contains(rexNode.getKind())) {
            return null;
        }
        final RexNode leftExp = ((RexCall) rexNode).getOperands().get(0);
        final RelDataType relDataType = filter.getInput().getRowType();
        boolean rightNotContain = true;
        for (int i = 1; i < ((RexCall) rexNode).getOperands().size(); i++) {
            if (!PredictRexShuttle.predicts(((RexCall) rexNode).getOperands().get(i)).isEmpty()) {
                rightNotContain = false;
                break;
            }
        }
        return PartitionFieldFounder.contains(leftExp, relDataType, partitionFields)
                        && rightNotContain
                ? leftExp
                : null;
    }

    /** PartitionFieldFounder. */
    private static class PartitionFieldFounder extends RexShuttle {

        private final RelDataType relDataType;
        private final List<String> partitionFields;
        private final AtomicBoolean contains = new AtomicBoolean(false);

        public PartitionFieldFounder(RelDataType relDataType, List<String> partitionFields) {
            this.relDataType = relDataType;
            this.partitionFields = partitionFields;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (partitionFields.contains(relDataType.getFieldNames().get(inputRef.getIndex()))) {
                contains.set(true);
            }
            return inputRef;
        }

        /**
         * check rexNode contains partition fields.
         *
         * @param rexNode reNode
         * @param relDataType relDataType
         * @param partitionFields partitionFields
         * @return true if contains
         */
        public static boolean contains(
                RexNode rexNode, RelDataType relDataType, List<String> partitionFields) {

            final PartitionFieldFounder fieldFounder =
                    new PartitionFieldFounder(relDataType, partitionFields);
            rexNode.accept(fieldFounder);
            return fieldFounder.contains.get();
        }
    }
}
