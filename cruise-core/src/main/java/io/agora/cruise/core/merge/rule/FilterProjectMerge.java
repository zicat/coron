package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;

import static io.agora.cruise.core.merge.Operand.ENY_NODE_TYPE;

/** FilterProjectMerge. */
public class FilterProjectMerge extends MergeRule {

    final ProjectMergeRule projectMergeRule = new ProjectMergeRule(ProjectMergeRule.Config.DEFAULT);

    public FilterProjectMerge(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        return projectMergeRule.merge(fromNode.getParent(), toNode, childrenResultNode);
    }

    /** FilterProjectMerge Config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                Operand.of(Filter.class, Project.class)
                                        .operand(Operand.of(Project.class, ENY_NODE_TYPE)))
                        .as(Config.class);

        @Override
        public FilterProjectMerge toMergeRule() {
            return new FilterProjectMerge(this);
        }
    }
}
