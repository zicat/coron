package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;

/** JoinMergeRule. */
public class JoinMergeRule extends MergeRule {

    public JoinMergeRule(MergeConfig mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        if (childrenResultNode.size() != 2) {
            return null;
        }
        final Join fromJoin = (Join) fromNode.getPayload();
        final Join toJoin = (Join) toNode.getPayload();

        if (!fromJoin.getLeft().deepEquals(toJoin.getLeft())) {
            return null;
        }
        if (!fromJoin.getRight().deepEquals(toJoin.getRight())) {
            return null;
        }
        if (fromJoin.isSemiJoin() != toJoin.isSemiJoin()) {
            return null;
        }
        if (fromJoin.getJoinType() != toJoin.getJoinType()) {
            return null;
        }
        if (!fromJoin.getRowType().equals(toJoin.getRowType())) {
            return null;
        }
        if (!fromJoin.getCondition().equals(toJoin.getCondition())) {
            return null;
        }
        return copy(fromJoin, childrenResultNode);
    }

    /** Join Config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT = new Config(Join.class, Join.class);

        public Config(Class<Join> fromRelNodeType, Class<Join> toRelNodeType) {
            super(fromRelNodeType, toRelNodeType);
        }

        @Override
        public JoinMergeRule toMergeRule() {
            return new JoinMergeRule(this);
        }
    }
}
