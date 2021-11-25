package io.agora.cruise.core.merge;

import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

/** TableScanMergeable. */
public class TableScanMergeable {

    /**
     * merge from scan and to scan with children node.
     *
     * @param fromScan from scan
     * @param toScan to scan
     * @param childrenResultNode children node
     * @return new table scan
     */
    public static RelNode merge(
            TableScan fromScan, TableScan toScan, ResultNodeList<RelNode> childrenResultNode) {
        final TableScan newScan = fromScan.getTable().equals(toScan.getTable()) ? fromScan : null;
        return RelNodeMergeable.copy(newScan, childrenResultNode);
    }
}
