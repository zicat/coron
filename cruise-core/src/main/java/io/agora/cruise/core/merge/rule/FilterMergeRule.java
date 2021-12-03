package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Arrays;
import java.util.List;

/** FilterMergeRule. */
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

        if (childrenResultNode.size() != 1) {
            return null;
        }

        final Filter fromFilter = (Filter) fromNode.getPayload();
        final Filter toFilter = (Filter) toNode.getPayload();
        final RelNode newInput = childrenResultNode.get(0).getPayload();
        final RexNode newFromCondition =
                createNewInputRexNode(fromFilter.getCondition(), fromFilter.getInput(), newInput);
        final RexNode newToCondition =
                createNewInputRexNode(toFilter.getCondition(), toFilter.getInput(), newInput);
        final List<RexNode> orList = Arrays.asList(newFromCondition, newToCondition);
        orList.sort((o1, o2) -> o2.toString().compareTo(o1.toString()));

        // sort condition to make output result uniqueness
        final RexNode newCondition = RexUtil.composeDisjunction(rexBuilder, orList);
        return fromFilter.copy(fromFilter.getTraitSet(), newInput, newCondition);
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
