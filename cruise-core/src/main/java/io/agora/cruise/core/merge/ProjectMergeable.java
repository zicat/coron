package io.agora.cruise.core.merge;

import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/** ProjectMergeable. */
public class ProjectMergeable {

    /**
     * merge from project and to project with children node.
     *
     * @param fromProject from project
     * @param toProject to project
     * @param childrenResultNode children node
     * @return new project
     */
    public static RelNode merge(
            Project fromProject, Project toProject, ResultNodeList<RelNode> childrenResultNode) {

        RelTraitSet newRelTraitSet = fromProject.getTraitSet().merge(toProject.getTraitSet());
        // first: add all from project field and field type
        List<RexNode> newProjects = new ArrayList<>(fromProject.getProjects());
        List<RelDataTypeField> newFields = new ArrayList<>(fromProject.getRowType().getFieldList());

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

        Project newProject =
                fromProject.copy(
                        newRelTraitSet,
                        fromProject.getInput(),
                        newProjects,
                        new RelRecordType(newFields));
        return RelNodeMergeable.copy(newProject, childrenResultNode);
    }

    /**
     * find field in field list, only check name and type.
     *
     * @param fields fields
     * @param field field
     * @return -1 if not found, else return position
     */
    private static int containsField(List<RelDataTypeField> fields, RelDataTypeField field) {
        for (int i = 0; i < fields.size(); i++) {
            RelDataTypeField oneField = fields.get(i);
            if (oneField.getName().equals(field.getName())
                    && oneField.getType().equals(field.getType())) {
                return i;
            }
        }
        return -1;
    }
}
