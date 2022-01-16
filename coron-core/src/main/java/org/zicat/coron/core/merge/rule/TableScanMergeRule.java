/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.core.merge.rule;

import org.zicat.coron.core.Node;
import org.zicat.coron.core.ResultNodeList;
import org.zicat.coron.core.merge.MergeConfig;
import org.zicat.coron.core.merge.Operand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

/** TableScanMergeRule. */
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

        if (mergeConfig.canMaterialized() && containsAggregate(fromScan)) {
            return null;
        }
        return copy(newScan, childrenResultNode);
    }

    /**
     * merge fromNode to toNode.
     *
     * @param fromNode fromNode
     * @param toNode toNode
     * @return TableScan
     */
    protected TableScan merge(RelNode fromNode, RelNode toNode) {
        final TableScan fromScan = (TableScan) fromNode;
        final TableScan toScan = (TableScan) toNode;
        return fromScan.deepEquals(toScan) ? fromScan : null;
    }

    /** TableScanMergeRule config. */
    public static class Config extends MergeConfig {

        public static Config create() {
            return new Config()
                    .withOperandSupplier(Operand.of(TableScan.class, TableScan.class))
                    .as(Config.class);
        }

        @Override
        public TableScanMergeRule toMergeRule() {
            return new TableScanMergeRule(this);
        }
    }
}
