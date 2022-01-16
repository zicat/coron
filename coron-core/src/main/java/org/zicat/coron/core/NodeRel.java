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

package org.zicat.coron.core;

import org.apache.calcite.rel.RelNode;
import org.zicat.coron.core.merge.RelNodeMergePlanner;

/** NodeRel. */
public class NodeRel extends Node<RelNode> {

    protected final RelNodeMergePlanner mergePlanner;

    protected NodeRel(RelNodeMergePlanner mergePlanner, Node<RelNode> parent, RelNode payload) {
        super(parent, payload);
        this.mergePlanner = mergePlanner;
    }

    /**
     * create RodeRel.
     *
     * @param mergePlanner mergePlanner
     * @param parent parent
     * @param payload payload
     * @return NodeRel
     */
    public static NodeRel of(
            RelNodeMergePlanner mergePlanner, Node<RelNode> parent, RelNode payload) {
        return new NodeRel(mergePlanner, parent, payload);
    }

    /**
     * create RodeRel.
     *
     * @param mergePlanner mergePlanner
     * @param payload payload
     * @return NodeRel
     */
    public static NodeRel of(RelNodeMergePlanner mergePlanner, RelNode payload) {
        return of(mergePlanner, null, payload);
    }

    /**
     * boolean is similar.
     *
     * @param toNode other node
     * @return boolean
     */
    @Override
    public ResultNode<RelNode> merge(
            Node<RelNode> toNode, ResultNodeList<RelNode> childrenResultNode) {

        if (toNode == null) {
            return ResultNode.of(childrenResultNode);
        }
        return mergePlanner.merge(this, toNode, childrenResultNode);
    }
}
