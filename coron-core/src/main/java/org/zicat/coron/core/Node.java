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

import java.util.ArrayList;
import java.util.List;

/** Node. */
public class Node<T> {

    protected final Node<T> parent;
    protected final List<Node<T>> children = new ArrayList<>();
    protected final T payload;
    protected final int indexInParent;

    protected Node(T payload) {
        this(null, payload);
    }

    protected Node(Node<T> parent, T payload) {
        if (payload == null) {
            throw new IllegalArgumentException("pay load is null");
        }
        this.parent = parent;
        this.payload = payload;
        this.indexInParent = isRoot() ? -1 : parent.childrenSize();
        if (parent != null) {
            parent.addChild(this);
        }
    }

    /**
     * get children size, if leaf return 0.
     *
     * @return size
     */
    public final int childrenSize() {
        return isLeaf() ? 0 : children.size();
    }

    /**
     * add child.
     *
     * @param child child node
     */
    public final void addChild(Node<T> child) {
        children.add(child);
    }

    /**
     * is leaf node.
     *
     * @return boolean
     */
    public final boolean isLeaf() {
        return children.isEmpty();
    }

    /**
     * is root node.
     *
     * @return boolean
     */
    public final boolean isRoot() {
        return parent == null;
    }

    /**
     * get payload.
     *
     * @return t
     */
    public final T getPayload() {
        return payload;
    }

    /**
     * check node mergeable subClass can override merge logic.
     *
     * @param otherNode other node
     * @param childrenResultNode childrenResultNode
     * @return ResultNode
     */
    public ResultNode<T> merge(Node<T> otherNode, ResultNodeList<T> childrenResultNode) {
        if (otherNode == null) {
            return ResultNode.of(null, childrenResultNode);
        }
        final T newPayload = payload.equals(otherNode.getPayload()) ? payload : null;
        return ResultNode.of(newPayload, childrenResultNode);
    }

    /**
     * boolean have the similar parent.
     *
     * @param otherNode other node
     * @return result node
     */
    public final ResultNode<T> parentMerge(
            Node<T> otherNode, ResultNodeList<T> childrenResultNode) {
        if (otherNode == null || isRoot() || otherNode.isRoot()) {
            return ResultNode.of(null, childrenResultNode);
        }
        return parent.merge(otherNode.parent, childrenResultNode);
    }

    /**
     * merge leaf node.
     *
     * @param otherNode other node.
     * @return result node
     */
    public final ResultNode<T> merge(Node<T> otherNode) {
        return merge(otherNode, null);
    }

    /**
     * return right brother size.
     *
     * @return size
     */
    public final int rightBrotherSize() {
        if (isRoot()) {
            return -1;
        }
        return parent.children.size() - getIndexInParent() - 1;
    }

    /**
     * get index in parent.
     *
     * @return index
     */
    public final int getIndexInParent() {
        if (isRoot()) {
            throw new UnsupportedOperationException("root has no brother");
        }
        return indexInParent;
    }

    /**
     * get parent.
     *
     * @return parent node
     */
    public final Node<T> getParent() {
        return parent;
    }

    /**
     * get right brother from index.
     *
     * @param i index
     * @return node
     */
    public final Node<T> rightBrother(int i) {
        int offset = getIndexInParent() + i + 1;
        if (offset >= parent.children.size()) {
            return null;
        }
        return parent.children.get(offset);
    }
}
