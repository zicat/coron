package io.agora.cruise.core.merge;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** FilterMergeable. */
public class FilterMergeable {

    /**
     * merge from filter and to filter with children node.
     *
     * @param fromFilter from filter
     * @param toFilter to filter
     * @param childrenResultNode children node.
     * @return new filter
     */
    public static RelNode merge(
            Filter fromFilter,
            Filter toFilter,
            ResultNodeList<RelNode> childrenResultNode,
            RexBuilder rexBuilder) {

        final RexNode newCondition =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.OR,
                        ImmutableList.of(fromFilter.getCondition(), toFilter.getCondition()));
        final Filter newFilter =
                fromFilter.copy(fromFilter.getTraitSet(), fromFilter.getInput(), newCondition);
        return RelNodeMergeable.copy(newFilter, childrenResultNode);
    }
}
