package io.agora.cruise.analyzer.shuttle;

import io.agora.cruise.analyzer.util.Lists;
import io.agora.cruise.core.util.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/** PartitionFilterSimplify. */
public class PartitionProjectFilterRelShuttle extends PartitionRelShuttle {

    PartitionProjectFilterRelShuttle(List<String> partitionFields) {
        super(partitionFields);
    }

    @Override
    public RelNode visit(LogicalProject project) {

        final RelNode newNode = super.visit(project);
        if (!(newNode instanceof Project) || !(((Project) newNode).getInput() instanceof Filter)) {
            return super.visit(newNode);
        }
        final Project newProject = (Project) newNode;
        final Filter filter = (Filter) newProject.getInput();

        final Tuple2<RelNode, List<RexNode>> tuple = transFilterCondition(filter);
        if (tuple == null) {
            return super.visit(newProject);
        }

        final List<RelDataTypeField> newRelTypeFields =
                new ArrayList<>(newProject.getRowType().getFieldList());

        for (int i = 0; i < tuple.f1.size(); i++) {
            final RexNode rexNode = tuple.f1.get(i);
            final int index = i + newRelTypeFields.size();
            final String name = getPrefixName() + index;
            newRelTypeFields.add(new RelDataTypeFieldImpl(name, index, rexNode.getType()));
        }

        return newProject.copy(
                newProject.getTraitSet(),
                tuple.f0,
                Lists.merge(newProject.getProjects(), tuple.f1),
                new RelRecordType(newRelTypeFields));
    }
}
