package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;

import static io.agora.cruise.core.merge.Operand.ENY_NODE_TYPE;

/** FilterProjectMergeRule. */
public class FilterProjectMergeRule extends MergeRule {

    final ProjectMergeRule projectMergeRule = ProjectMergeRule.Config.DEFAULT.toMergeRule();

    public FilterProjectMergeRule(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        return projectMergeRule.merge(fromNode.getParent(), toNode, childrenResultNode);
    }

    /** FilterProjectMergeRule Config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(
                                Operand.of(Filter.class, Project.class)
                                        .operand(Operand.of(Project.class, ENY_NODE_TYPE)))
                        .as(Config.class);

        @Override
        public FilterProjectMergeRule toMergeRule() {
            return new FilterProjectMergeRule(this);
        }
    }
}
