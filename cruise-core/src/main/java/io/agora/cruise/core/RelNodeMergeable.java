package io.agora.cruise.core;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;

/** RelNodeSimilar. */
public class RelNodeMergeable {

    final RexBuilder rexBuilder = new RexBuilder(CalciteContext.sqlTypeFactory());

    public RelNode merge(Filter fromFilter, Filter toFilter) {
        final RexNode newCondition =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.OR,
                        ImmutableList.of(fromFilter.getCondition(), toFilter.getCondition()));
        return fromFilter.copy(fromFilter.getTraitSet(), fromFilter.getInput(), newCondition);
    }

    public RelNode merge(TableScan fromScan, TableScan toScan) {
        return fromScan.getTable().equals(toScan.getTable()) ? fromScan : null;
    }

    public RelNode merge(Project fromProject, Project toProject) {
        if (!fromProject.getInput().getRowType().equals(toProject.getInput().getRowType())) {
            return null;
        }
        RelTraitSet newRelTraitSet = fromProject.getTraitSet().merge(toProject.getTraitSet());
        List<RexNode> newProjects = new ArrayList<>(fromProject.getProjects());
        List<RelDataTypeField> newFields = new ArrayList<>();
        for (int i = 0; i < fromProject.getRowType().getFieldCount(); i++) {
            RelDataTypeField field =
                    fromProject
                            .getRowType()
                            .getField(fromProject.getRowType().getFieldNames().get(i), true, true);
            newFields.add(field);
        }
        for (int i = 0; i < toProject.getProjects().size(); i++) {
            RexNode rexNode = toProject.getProjects().get(i);
            if (!newProjects.contains(rexNode)) {
                newProjects.add(rexNode);
                RelDataTypeField field =
                        toProject
                                .getRowType()
                                .getField(
                                        toProject.getRowType().getFieldNames().get(i), true, true);
                newFields.add(field);
            }
        }

        return fromProject.copy(
                newRelTraitSet,
                fromProject.getInput(),
                newProjects,
                new RelRecordType(StructKind.FULLY_QUALIFIED, newFields));
    }

    public RelNode merge(Node<RelNode> fromNode, Node<RelNode> toNode) {
        final RelNode from = fromNode.payload;
        final RelNode to = toNode.payload;
        if (from instanceof Filter && to instanceof Filter) {
            return merge((Filter) from, (Filter) to);
        }
        if (from instanceof TableScan && to instanceof TableScan) {
            return merge((TableScan) from, (TableScan) to);
        }
        if (from instanceof Project && to instanceof Project) {
            return merge((Project) from, (Project) to);
        }
        return from.equals(to) ? from : null;
    }
}
