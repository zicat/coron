package io.agora.cruise.analyzer.simplify;

import io.agora.cruise.analyzer.util.Lists;
import io.agora.cruise.core.util.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/** PartitionFilterSimplify. */
public class PartitionFilterSimplify extends PartitionSimplify {

    public PartitionFilterSimplify(List<String> partitionFields) {
        super(partitionFields);
    }

    @Override
    public RelNode visit(LogicalProject project) {

        project = (LogicalProject) super.visit(project);
        if (!(project.getInput() instanceof Filter)) {
            return super.visit(project);
        }
        final Filter filter = (Filter) project.getInput();
        final Tuple2<RelNode, List<RexNode>> tuple = transFilterCondition(filter);
        if (tuple == null) {
            return super.visit(project);
        }

        final List<RelDataTypeField> newRelTypeFields =
                new ArrayList<>(project.getRowType().getFieldList());

        for (int i = 0; i < tuple.f1.size(); i++) {
            RexNode rexNode = tuple.f1.get(i);
            int index = i + newRelTypeFields.size();
            String name = getPrefixName() + index;
            newRelTypeFields.add(new RelDataTypeFieldImpl(name, index, rexNode.getType()));
        }

        return project.copy(
                project.getTraitSet(),
                tuple.f0,
                Lists.merge(project.getProjects(), tuple.f1),
                new RelRecordType(newRelTypeFields));
    }
}
