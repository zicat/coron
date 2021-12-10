package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;

/** AggregateFilterMergeRule. */
public class AggregateFilterMergeRule extends MergeRule {

    public AggregateFilterMergeRule(MergeConfig mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {
        // calcite is not support materialized aggregate with filter(having)
        return null;
    }

    /** Config. */
    public static class Config extends MergeConfig {

        public static Config createFrom() {
            return new Config()
                    .withOperandSupplier(
                            Operand.ofFrom(Aggregate.class).operand(Operand.ofFrom(Filter.class)))
                    .as(AggregateFilterMergeRule.Config.class);
        }

        public static Config createTo() {
            return new Config()
                    .withOperandSupplier(
                            Operand.ofTo(Aggregate.class).operand(Operand.ofTo(Filter.class)))
                    .as(AggregateFilterMergeRule.Config.class);
        }

        @Override
        public AggregateFilterMergeRule toMergeRule() {
            return new AggregateFilterMergeRule(this);
        }
    }
}
