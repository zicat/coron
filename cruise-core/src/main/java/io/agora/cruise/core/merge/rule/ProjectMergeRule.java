package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/** ProjectMergeable. */
public class ProjectMergeRule extends MergeRule<Project, Project> {

    public ProjectMergeRule(ProjectMergeRule.Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        final Project fromProject = (Project) fromNode.getPayload();
        final Project toProject = (Project) toNode.getPayload();

        final RelTraitSet newRelTraitSet = fromProject.getTraitSet().merge(toProject.getTraitSet());
        // first: add all from project field and field type
        final List<RexNode> newProjects = new ArrayList<>(fromProject.getProjects());
        final List<RelDataTypeField> newFields =
                new ArrayList<>(fromProject.getRowType().getFieldList());

        for (int i = 0; i < toProject.getProjects().size(); i++) {
            RexNode rexNode = toProject.getProjects().get(i);
            RelDataTypeField field = toProject.getRowType().getFieldList().get(i);
            int fieldNameContains = containsField(newFields, field);
            // alias name not exist, we can add this alias and it's RexNode
            if (fieldNameContains == -1) {
                newProjects.add(rexNode);
                newFields.add(field);
                continue;
            }
            if (newProjects.get(fieldNameContains).equals(rexNode)) {
                continue;
            }
            // important: once alias name is equal, RexNode not equal,
            // <p> parent RelNode fail to mapping id with name
            return null;
        }

        final Project newProject =
                fromProject.copy(
                        newRelTraitSet,
                        fromProject.getInput(),
                        newProjects,
                        new RelRecordType(newFields));
        return copy(newProject, childrenResultNode);
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
    public static class Config extends MergeConfig<Project, Project> {

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
