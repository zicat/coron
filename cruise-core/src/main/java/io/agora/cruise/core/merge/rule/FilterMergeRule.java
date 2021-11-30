package io.agora.cruise.core.merge.rule;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** FilterMergeable. */
public class FilterMergeRule extends MergeRule {

    final RexBuilder rexBuilder = new RexBuilder(CalciteContext.DEFAULT_SQL_TYPE_FACTORY);

    public FilterMergeRule(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        final Filter fromFilter = (Filter) fromNode.getPayload();
        final Filter toFilter = (Filter) toNode.getPayload();

        // sort condition to make output result uniqueness
        final RexNode newCondition =
                fromFilter.getCondition().toString().compareTo(toFilter.getCondition().toString())
                                > 0
                        ? rexBuilder.makeCall(
                                SqlStdOperatorTable.OR,
                                ImmutableList.of(
                                        fromFilter.getCondition(), toFilter.getCondition()))
                        : rexBuilder.makeCall(
                                SqlStdOperatorTable.OR,
                                ImmutableList.of(
                                        toFilter.getCondition(), fromFilter.getCondition()));
        final Filter newFilter =
                fromFilter.copy(fromFilter.getTraitSet(), fromFilter.getInput(), newCondition);
        return copy(newFilter, childrenResultNode);
    }

    /** Filter Config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(Operand.of(Filter.class, Filter.class))
                        .as(Config.class);

        @Override
        public FilterMergeRule toMergeRule() {
            return new FilterMergeRule(this);
        }
    }
}
