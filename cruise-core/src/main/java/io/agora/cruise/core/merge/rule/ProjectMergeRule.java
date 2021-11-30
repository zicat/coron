package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

/** ProjectMergeable. */
public class ProjectMergeRule extends MergeRule {

    public ProjectMergeRule(ProjectMergeRule.Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        // project only has one input
        if (childrenResultNode.size() != 1) {
            return null;
        }

        final Project fromProject = (Project) fromNode.getPayload();
        final Project toProject = (Project) toNode.getPayload();
        final RelNode newInput = childrenResultNode.get(0).getPayload();

        final RelTraitSet newRelTraitSet = fromProject.getTraitSet().merge(toProject.getTraitSet());
        // first: add all from project field and field type
        final List<RexNode> newProjects = new ArrayList<>();
        for (int i = 0; i < fromProject.getProjects().size(); i++) {
            RexNode rexNode = fromProject.getProjects().get(i);
            RexNode newRexNode = createNewInputRexNode(rexNode, fromProject.getInput(), newInput);
            newProjects.add(newRexNode);
        }

        final List<RelDataTypeField> newFields =
                new ArrayList<>(fromProject.getRowType().getFieldList());

        for (int i = 0; i < toProject.getProjects().size(); i++) {
            RexNode rexNode = toProject.getProjects().get(i);
            RexNode newRexNode = createNewInputRexNode(rexNode, toProject.getInput(), newInput);
            RelDataTypeField field = toProject.getRowType().getFieldList().get(i);
            int fieldNameContains = containsField(newFields, field);
            if (fieldNameContains == -1) {
                newProjects.add(newRexNode);
                newFields.add(field);
                continue;
            }

            if (newProjects.get(fieldNameContains).equals(newRexNode)) {
                continue;
            }
            // important: once alias name is equal, RexNode not equal,
            // <p> parent RelNode fail to mapping id with name
            return null;
        }

        return fromProject.copy(
                newRelTraitSet, newInput, newProjects, new RelRecordType(newFields));
    }

    /**
     * create new RexNode that inputRef replace from fromInput to newInput.
     *
     * @param rexNode rexNode
     * @param fromInput fromInput
     * @param toInput toInput
     * @return RexNode
     */
    private RexNode createNewInputRexNode(RexNode rexNode, RelNode fromInput, RelNode toInput) {
        return rexNode.accept(
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        int index = inputRef.getIndex();
                        String name = fromInput.getRowType().getFieldNames().get(index);
                        int newIndex = findIndexByName(toInput.getRowType(), name);
                        return newIndex == -1
                                ? inputRef
                                : new RexInputRef(newIndex, inputRef.getType());
                    }
                });
    }

    /**
     * find field in field list, only check name and type.
     *
     * @param fields fields
     * @param field field
     * @return -1 if not found, else return position
     */
    private int containsField(List<RelDataTypeField> fields, RelDataTypeField field) {
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField oneField = fields.get(i);
            if (oneField.getName().equals(field.getName())
                    && oneField.getType().equals(field.getType())) {
                return i;
            }
        }
        return -1;
    }

    /** project config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT = new Config(Project.class, Project.class);

        public Config(Class<Project> fromRelNodeType, Class<Project> toRelNodeType) {
            super(fromRelNodeType, toRelNodeType);
        }

        @Override
        public ProjectMergeRule toMergeRule() {
            return new ProjectMergeRule(this);
        }
    }
}
