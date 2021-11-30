package io.agora.cruise.core.merge.rule;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

/** TableScanMergeable. */
public class TableScanMergeRule extends MergeRule {

    public TableScanMergeRule(TableScanMergeRule.Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        final TableScan fromScan = (TableScan) fromNode.getPayload();
        final TableScan toScan = (TableScan) toNode.getPayload();

        final TableScan newScan = fromScan.deepEquals(toScan) ? fromScan : null;
        return copy(newScan, childrenResultNode);
    }

    /** table scan config. */
    public static class Config extends MergeConfig {

        public static final Config DEFAULT = new Config(TableScan.class, TableScan.class);

        public Config(Class<TableScan> fromRelNodeType, Class<TableScan> toRelNodeType) {
            super(fromRelNodeType, toRelNodeType);
        }

        @Override
        public TableScanMergeRule toMergeRule() {
            return new TableScanMergeRule(this);
        }
    }
}
