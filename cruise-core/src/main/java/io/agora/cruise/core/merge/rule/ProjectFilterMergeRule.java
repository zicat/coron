package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;

/** ProjectFilterMergeRule. */
public class ProjectFilterMergeRule extends MergeRule {

    final ProjectMergeRule projectMergeRule = ProjectMergeRule.Config.DEFAULT.toMergeRule();

    public ProjectFilterMergeRule(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        return projectMergeRule.merge(fromNode, toNode.getParent(), childrenResultNode);
    }

    /** ProjectFilterMergeRule Config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                Operand.of(Project.class, Filter.class)
                                        .operand(Operand.ofTo(Project.class)))
                        .as(Config.class);

        @Override
        public ProjectFilterMergeRule toMergeRule() {
            return new ProjectFilterMergeRule(this);
        }
    }
}
