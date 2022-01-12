package io.agora.cruise.core.merge.rule;

import com.google.common.collect.Maps;
import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** ProjectMergeRule. */
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
        if (mergeConfig.canMaterialized()
                && containsAggregate(newInput)
                && !(newInput instanceof Aggregate)) {
            return null;
        }

        final Project fromProject = (Project) fromNode;
        final Project toProject = (Project) toNode;
        final RelTraitSet newRelTraitSet = fromProject.getTraitSet().merge(toProject.getTraitSet());
        final int newFieldSize = fromProject.getProjects().size() + toProject.getProjects().size();
        // first: add all from project field and field type
        final Map<RelDataTypeField, RexNode> fieldMapping =
                Maps.newLinkedHashMapWithExpectedSize(newFieldSize);
        final Map<String, RexNode> nameMapping =
                Maps.newLinkedHashMapWithExpectedSize(newFieldSize);
        final Map<String, Integer> fieldIndexMapping = dataTypeNameIndex(newInput.getRowType());
        for (int i = 0; i < fromProject.getProjects().size(); i++) {
            final RexNode rexNode = fromProject.getProjects().get(i);
            final RexNode newRexNode =
                    createNewInputRexNode(rexNode, fromProject.getInput(), fieldIndexMapping);
            final RelDataTypeField field = fromProject.getRowType().getFieldList().get(i);
            fieldMapping.put(field, newRexNode);
            nameMapping.put(field.getName(), newRexNode);
        }

        for (int i = 0; i < toProject.getProjects().size(); i++) {
            final RexNode rexNode = toProject.getProjects().get(i);
            final RexNode newRexNode =
                    createNewInputRexNode(rexNode, toProject.getInput(), fieldIndexMapping);
            final RelDataTypeField field = toProject.getRowType().getFieldList().get(i);
            final RexNode fromRexNode = nameMapping.get(field.getName());
            if (fromRexNode == null) {
                fieldMapping.put(field, newRexNode);
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
        final List<RelDataTypeField> newFields = new ArrayList<>(fieldMapping.keySet());
        newFields.sort(Comparator.comparing(RelDataTypeField::getName));
        final List<RexNode> newRexNodes = new ArrayList<>(fieldMapping.size());
        newFields.forEach(field -> newRexNodes.add(fieldMapping.get(field)));
        return fromProject.copy(
                newRelTraitSet, newInput, newRexNodes, new RelRecordType(newFields));
    }

    /** project config. */
    public static class Config extends MergeConfig {

        public static Config create() {
            return new Config()
                    .withOperandSupplier(Operand.of(Project.class, Project.class))
                    .as(Config.class);
        }

        @Override
        public ProjectMergeRule toMergeRule() {
            return new ProjectMergeRule(this);
        }
    }
}
