package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.*;

/** ProjectMergeable. */
public class ProjectMergeRule extends MergeRule {

    public ProjectMergeRule(Config mergeConfig) {
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
        return merge(fromNode.getPayload(), toNode.getPayload(), childrenResultNode);
    }

    /**
     * merge from node and to node by new input.
     *
     * @param fromNode from nde
     * @param toNode to node
     * @param childrenResultNode child
     * @return new rel node
     */
    protected RelNode merge(
            RelNode fromNode, RelNode toNode, List<ResultNode<RelNode>> childrenResultNode) {

        final RelNode newInput = childrenResultNode.get(0).getPayload();
        final Project fromProject = (Project) fromNode;
        final Project toProject = (Project) toNode;
        final RelTraitSet newRelTraitSet = fromProject.getTraitSet().merge(toProject.getTraitSet());
        // first: add all from project field and field type
        final Map<RelDataTypeField, RexNode> projectRexMapping = new HashMap<>();
        for (int i = 0; i < fromProject.getProjects().size(); i++) {
            RexNode rexNode = fromProject.getProjects().get(i);
            RexNode newRexNode = createNewInputRexNode(rexNode, fromProject.getInput(), newInput);
            projectRexMapping.put(fromProject.getRowType().getFieldList().get(i), newRexNode);
        }

        for (int i = 0; i < toProject.getProjects().size(); i++) {
            RexNode rexNode = toProject.getProjects().get(i);
            RexNode newRexNode = createNewInputRexNode(rexNode, toProject.getInput(), newInput);
            RelDataTypeField field = toProject.getRowType().getFieldList().get(i);
            RexNode fromRexNode = findRexNode(projectRexMapping, field);
            if (fromRexNode == null) {
                projectRexMapping.put(field, newRexNode);
                continue;
            }

            if (fromRexNode.equals(newRexNode)) {
                continue;
            }
            // important: once alias name is equal, RexNode not equal,
            // <p> parent RelNode fail to mapping id with name
            return null;
        }

        // sort field to make output result uniqueness
        final List<RelDataTypeField> newFields = new ArrayList<>(projectRexMapping.keySet());
        newFields.sort(Comparator.comparing(RelDataTypeField::getName));
        final List<RexNode> newRexNodes = new ArrayList<>();
        newFields.forEach(field -> newRexNodes.add(projectRexMapping.get(field)));

        return fromProject.copy(
                newRelTraitSet, newInput, newRexNodes, new RelRecordType(newFields));
    }

    /**
     * found RexNode by name.
     *
     * @param project projects
     * @param field field
     * @return null if not found else return RexNode
     */
    private RexNode findRexNode(Map<RelDataTypeField, RexNode> project, RelDataTypeField field) {
        for (Map.Entry<RelDataTypeField, RexNode> entry : project.entrySet()) {
            final RelDataTypeField oneField = entry.getKey();
            final RexNode value = entry.getValue();
            if (oneField.getName().equals(field.getName())) {
                return value;
            }
        }
        return null;
    }

    /** project config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(Operand.of(Project.class, Project.class))
                        .as(Config.class);

        @Override
        public ProjectMergeRule toMergeRule() {
            return new ProjectMergeRule(this);
        }
    }
}
