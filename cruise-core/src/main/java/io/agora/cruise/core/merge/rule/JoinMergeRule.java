package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;

/** JoinMergeRule. */
public class JoinMergeRule extends MergeRule {

    final ProjectMergeRule projectMergeRule = ProjectMergeRule.Config.DEFAULT.toMergeRule();
    final TableScanMergeRule tableScanMergeRule = TableScanMergeRule.Config.DEFAULT.toMergeRule();

    public JoinMergeRule(Config mergeConfig) {
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

        final RelNode newLeft =
                merge(fromJoin.getLeft(), toJoin.getLeft(), childrenResultNode.get(0));
        if (newLeft == null) {
            return null;
        }
        final RelNode newRight =
                merge(fromJoin.getRight(), toJoin.getRight(), childrenResultNode.get(1));
        if (newRight == null) {
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
        fromJoin.copy(
                fromJoin.getTraitSet(),
                fromJoin.getCondition(),
                newLeft,
                newRight,
                fromJoin.getJoinType(),
                fromJoin.isSemiJoin());
        return copy(fromJoin, childrenResultNode);
    }

    /**
     * merge logic.
     *
     * @param from from
     * @param to to
     * @param child newInput
     * @return new RelNode
     */
    private RelNode merge(RelNode from, RelNode to, ResultNode<RelNode> child) {
        if (from instanceof TableScan && to instanceof TableScan) {
            return tableScanMergeRule.merge(from, to);
        } else if (from instanceof Project && to instanceof Project) {
            return projectMergeRule.merge(from, to, child.getChildren());
        }
        return null;
    }

    /** Join Config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(Operand.of(Join.class, Join.class))
                        .as(Config.class);

        @Override
        public JoinMergeRule toMergeRule() {
            return new JoinMergeRule(this);
        }
    }
}
