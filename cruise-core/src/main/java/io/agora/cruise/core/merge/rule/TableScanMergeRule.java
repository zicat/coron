package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

/** TableScanMergeable. */
public class TableScanMergeRule extends MergeRule {

    public TableScanMergeRule(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        final TableScan fromScan = (TableScan) fromNode.getPayload();
        final TableScan toScan = (TableScan) toNode.getPayload();
        final TableScan newScan = merge(fromScan, toScan);
        return copy(newScan, childrenResultNode);
    }

    protected TableScan merge(RelNode fromNode, RelNode toNode) {
        final TableScan fromScan = (TableScan) fromNode;
        final TableScan toScan = (TableScan) toNode;
        return fromScan.deepEquals(toScan) ? fromScan : null;
    }

    /** table scan config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT =
                new Config()
                        .withOperandSupplier(Operand.of(TableScan.class, TableScan.class))
                        .as(Config.class);

        @Override
        public TableScanMergeRule toMergeRule() {
            return new TableScanMergeRule(this);
        }
    }
}
