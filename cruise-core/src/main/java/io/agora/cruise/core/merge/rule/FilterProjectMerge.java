package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.TwoMergeType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;

/** FilterProjectMerge. */
public class FilterProjectMerge extends MergeRule {

    final ProjectMergeRule projectMergeRule = new ProjectMergeRule(ProjectMergeRule.Config.DEFAULT);

    public FilterProjectMerge(MergeConfig mergeConfig) {
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

        public static final Config DEFAULT = new Config(Filter.class, Project.class);

        public Config(Class<Filter> fromRelNodeType, Class<Project> toRelNodeType) {
            super(fromRelNodeType, toRelNodeType, new TwoMergeType(Project.class, RelNode.class));
        }

        @Override
        public FilterProjectMerge toMergeRule() {
            return new FilterProjectMerge(this);
        }
    }
}
