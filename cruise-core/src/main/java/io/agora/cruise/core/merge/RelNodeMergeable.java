package io.agora.cruise.core.merge;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;

import java.util.ArrayList;
import java.util.List;

/** RelNodeSimilar. */
public class RelNodeMergeable {

    final RexBuilder rexBuilder = new RexBuilder(CalciteContext.sqlTypeFactory());

    /**
     * merge from node and to node with children inputs.
     *
     * @param fromNode from node
     * @param toNode to node
     * @param childrenResultNode children inputs
     * @return new rel node
     */
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        final RelNode from = fromNode.getPayload();
        final RelNode to = toNode.getPayload();
        if (from instanceof Filter && to instanceof Filter) {
            return FilterMergeable.merge(
                    (Filter) from, (Filter) to, childrenResultNode, rexBuilder);
        }
        if (from instanceof TableScan && to instanceof TableScan) {
            return TableScanMergeable.merge((TableScan) from, (TableScan) to, childrenResultNode);
        }
        if (from instanceof Project && to instanceof Project) {
            return ProjectMergeable.merge((Project) from, (Project) to, childrenResultNode);
        }
        if (from instanceof Aggregate && to instanceof Aggregate) {
            return AggregationMergeable.merge((Aggregate) from, (Aggregate) to, childrenResultNode);
        }
        return copy(from.equals(to) ? from : null, childrenResultNode);
    }

    /**
     * copy input of RelNode with children in result node.
     *
     * @param relNode RelNode
     * @param childrenResultNode childrenResultNode
     * @return rel
     */
    public static RelNode copy(RelNode relNode, ResultNodeList<RelNode> childrenResultNode) {
        if (childrenResultNode == null || childrenResultNode.isEmpty()) {
            return relNode;
        }
        if (relNode == null) {
            return null;
        }
        List<RelNode> inputs = new ArrayList<>();
        for (ResultNode<RelNode> resultNode : childrenResultNode) {
            inputs.add(resultNode.getPayload());
        }
        return relNode.copy(relNode.getTraitSet(), inputs);
    }
}
