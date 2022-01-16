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

package org.zicat.coron.analyzer.module;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.zicat.coron.core.Node;
import org.zicat.coron.core.NodeRel;

import java.util.List;
import java.util.Set;

import static org.zicat.coron.core.NodeUtils.findAllFirstLeafNode;

/** NodeRelMeta. */
public final class NodeRelMeta {

    private static final NodeRelMeta EMPTY = new NodeRelMeta();

    private final NodeRel nodeRel;
    private final Set<String> tables;
    private final List<Node<RelNode>> leafNodes;

    private NodeRelMeta() {
        this.nodeRel = null;
        this.tables = null;
        this.leafNodes = null;
    }

    public NodeRelMeta(NodeRel nodeRel) {
        this.nodeRel = nodeRel;
        this.tables = TableRelShuttleImpl.tables(nodeRel.getPayload());
        this.leafNodes = findAllFirstLeafNode(nodeRel);
    }

    public final NodeRel nodeRel() {
        return nodeRel;
    }

    public final Set<String> tables() {
        return tables;
    }

    public final List<Node<RelNode>> leafNodes() {
        return leafNodes;
    }

    /**
     * create empty instance.
     *
     * @return NodeRelMeta
     */
    public static NodeRelMeta empty() {
        return EMPTY;
    }

    /**
     * check if empty.
     *
     * @return boolean
     */
    public final boolean isEmpty() {
        return this == EMPTY;
    }

    /**
     * isTableIntersect.
     *
     * @param from from
     * @param to to
     * @return boolean intersectTables
     */
    public static boolean isTableIntersect(NodeRelMeta from, NodeRelMeta to) {
        for (String a : from.tables()) {
            if (to.tables.contains(a)) {
                return true;
            }
        }
        return false;
    }
}
