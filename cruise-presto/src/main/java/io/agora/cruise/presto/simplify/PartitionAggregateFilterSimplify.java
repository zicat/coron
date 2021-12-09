package io.agora.cruise.presto.simplify;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.core.NodeRel;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PartitionAggregateFilterSimplify.
 *
 * <p>trans: select sum(x) where f(t) = 1 to select t,sum(x) group by f(t). (t in partitionFields)
 */
public class PartitionAggregateFilterSimplify implements NodeRel.Simplify {

    final RexBuilder rexBuilder = new RexBuilder(CalciteContext.DEFAULT_SQL_TYPE_FACTORY);

    private static final List<SqlKind> COMPARE_KIND =
            Arrays.asList(
                    SqlKind.EQUALS,
                    SqlKind.BETWEEN,
                    SqlKind.LESS_THAN,
                    SqlKind.LESS_THAN_OR_EQUAL,
                    SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.IN);

    private final List<String> partitionFields;
    private final RexNode alwaysTrue;

    public PartitionAggregateFilterSimplify(List<String> partitionFields) {
        this.partitionFields = partitionFields;
        this.alwaysTrue =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        Arrays.asList(rexBuilder.makeLiteral("1"), rexBuilder.makeLiteral("1")));
    }

    @Override
    public RelNode apply(RelNode relNode) {
        if (partitionFields == null || partitionFields.isEmpty()) {
            return relNode;
        }
        return relNode.accept(
                new RelShuttleImpl() {
                    @Override
                    public RelNode visit(LogicalAggregate aggregate) {

                        aggregate = (LogicalAggregate) super.visit(aggregate);

                        if (!(aggregate.getInput() instanceof Project)) {
                            return super.visit(aggregate);
                        }

                        if (!aggregate
                                .getGroupSets()
                                .equals(ImmutableList.of(aggregate.getGroupSet()))) {
                            return super.visit(aggregate);
                        }
                        Project project = (Project) aggregate.getInput();
                        if (project.getInput() == null || !(project.getInput() instanceof Filter)) {
                            return super.visit(aggregate);
                        }
                        Filter filter = (Filter) project.getInput();
                        if (filter.getCondition().getKind() != SqlKind.AND) {
                            return super.visit(aggregate);
                        }
                        Tuple2<Filter, List<RexNode>> tuple = getFilterRexNode(filter);
                        if (tuple == null) {
                            return super.visit(aggregate);
                        }

                        List<RexNode> newProjects = new ArrayList<>(project.getProjects());
                        newProjects.addAll(tuple.f1);
                        List<RelDataTypeField> newRelTypeFields =
                                new ArrayList<>(project.getRowType().getFieldList());
                        List<Integer> newGroupSet =
                                new ArrayList<>(aggregate.getGroupSet().asList());
                        List<String> newFieldNames = new ArrayList<>();
                        for (int i = 0; i < tuple.f1.size(); i++) {
                            RexNode rexNode = tuple.f1.get(i);
                            int index = i + project.getProjects().size();
                            String name = "tmp_p_" + index;
                            newRelTypeFields.add(
                                    new RelDataTypeFieldImpl(name, index, rexNode.getType()));
                            newFieldNames.add(name);
                            newGroupSet.add(index);
                        }
                        Project newProject =
                                project.copy(
                                        project.getTraitSet(),
                                        tuple.f0,
                                        newProjects,
                                        new RelRecordType(newRelTypeFields));
                        Aggregate newAggregate =
                                aggregate.copy(
                                        aggregate.getTraitSet(),
                                        newProject,
                                        ImmutableBitSet.of(newGroupSet),
                                        null,
                                        aggregate.getAggCallList());

                        List<String> allProjectName =
                                new ArrayList<>(aggregate.getRowType().getFieldNames());
                        allProjectName.addAll(newFieldNames);
                        List<RexNode> allProjectNodes = new ArrayList<>();
                        for (String name : allProjectName) {
                            int i = foundId(newAggregate.getRowType().getFieldNames(), name);
                            if (i != -1) {
                                allProjectNodes.add(rexBuilder.makeInputRef(newAggregate, i));
                                continue;
                            }
                            if (name.startsWith("$f")) {
                                int index = Integer.parseInt(name.replace("$f", ""));
                                name = "$f" + (index + newFieldNames.size());
                            }
                            allProjectNodes.add(
                                    rexBuilder.makeInputRef(
                                            newAggregate,
                                            foundId(
                                                    newAggregate.getRowType().getFieldNames(),
                                                    name)));
                        }

                        return LogicalProject.create(
                                newAggregate, new ArrayList<>(), allProjectNodes, allProjectName);
                    }
                });
    }

    private int foundId(List<String> list, String value) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).equals(value)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * get filter rex node.
     *
     * @param filter filter
     * @return tuple2
     */
    private Tuple2<Filter, List<RexNode>> getFilterRexNode(Filter filter) {
        final RexNode rexNode = filter.getCondition();
        RexNode leftExp = leftExpParser(rexNode, filter);
        if (leftExp != null) {
            return new Tuple2<>(
                    filter.copy(filter.getTraitSet(), filter.getInput(), alwaysTrue),
                    Collections.singletonList(leftExp));
        }
        if (rexNode.getKind() == SqlKind.AND) {
            RexCall rexCall = (RexCall) rexNode;
            List<RexNode> newConditions = new ArrayList<>();
            List<RexNode> resultRexNode = new ArrayList<>();
            for (int i = 0; i < rexCall.getOperands().size(); i++) {
                RexNode childNode = rexCall.getOperands().get(i);
                RexNode childLeftExp = leftExpParser(rexCall.getOperands().get(i), filter);
                if (childLeftExp == null) {
                    newConditions.add(childNode);
                } else {
                    if (!resultRexNode.contains(childLeftExp)) {
                        resultRexNode.add(childLeftExp);
                    }
                }
            }
            if (resultRexNode.isEmpty()) {
                return null;
            }
            if (newConditions.isEmpty()) {
                return new Tuple2<>(
                        filter.copy(filter.getTraitSet(), filter.getInput(), alwaysTrue),
                        resultRexNode);
            } else if (newConditions.size() == 1) {
                return new Tuple2<>(
                        filter.copy(filter.getTraitSet(), filter.getInput(), newConditions.get(0)),
                        resultRexNode);
            } else {
                return new Tuple2<>(
                        filter.copy(
                                filter.getTraitSet(),
                                filter.getInput(),
                                rexCall.clone(rexCall.type, newConditions)),
                        resultRexNode);
            }
        }

        return null;
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
        final AtomicBoolean contains = new AtomicBoolean(false);
        final RexNode leftExp = ((RexCall) rexNode).getOperands().get(0);
        leftExp.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        if (partitionFields.contains(
                                filter.getInput()
                                        .getRowType()
                                        .getFieldNames()
                                        .get(inputRef.getIndex()))) {
                            contains.set(true);
                        }
                        return inputRef;
                    }
                });
        return contains.get() ? leftExp : null;
    }
}
