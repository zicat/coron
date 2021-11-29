package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.TwoMergeType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;

/** ProjectFilterMerge. */
public class ProjectFilterMerge extends MergeRule<Project, Filter> {

    final ProjectMergeRule projectMergeRule = new ProjectMergeRule(ProjectMergeRule.Config.DEFAULT);

    public ProjectFilterMerge(MergeConfig<Project, Filter> mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        return projectMergeRule.merge(fromNode, toNode.getParent(), childrenResultNode);
    }

    /** ProjectFilterMerge Config. */
    public static class Config extends MergeConfig<Project, Filter> {

        public static final Config DEFAULT = new Config(Project.class, Filter.class);

        public Config(Class<Project> fromRelNodeType, Class<Filter> toRelNodeType) {
            super(fromRelNodeType, toRelNodeType, new TwoMergeType<>(RelNode.class, Project.class));
        }

        @Override
        public ProjectFilterMerge toMergeRule() {
            return new ProjectFilterMerge(this);
        }
    }
}
