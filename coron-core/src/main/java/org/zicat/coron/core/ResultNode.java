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

import java.util.List;

/**
 * ResultNode.
 *
 * @param <T>
 */
public class ResultNode<T> {

    protected ResultNode<T> parent;
    protected final List<ResultNode<T>> children;
    protected final T payload;
    protected int fromLookAhead = 1;
    protected int toLookAhead = 1;

    private ResultNode(T payload, List<ResultNode<T>> children) {
        this.payload = payload;
        this.children = children;
        if (children != null) {
            children.forEach(child -> child.setParent(this));
        }
    }

    /**
     * set from node look ahead size.
     *
     * @param fromLookAhead fromLookAhead
     */
    public void setFromLookAhead(int fromLookAhead) {
        this.fromLookAhead = fromLookAhead;
    }

    /**
     * set to node look ahead size.
     *
     * @param toLookAhead toLookAhead
     */
    public void setToLookAhead(int toLookAhead) {
        this.toLookAhead = toLookAhead;
    }

    /**
     * Create result node.
     *
     * @param payload payload
     * @param children children
     * @param <T> type
     * @return result node.
     */
    public static <T> ResultNode<T> of(T payload, List<ResultNode<T>> children) {
        return new ResultNode<>(payload, children);
    }

    /**
     * Create result node with null payload.
     *
     * @param children children
     * @param <T> type
     * @return result node.
     */
    public static <T> ResultNode<T> of(List<ResultNode<T>> children) {
        return new ResultNode<>(null, children);
    }

    /**
     * create empty result node.
     *
     * @param <T> type
     * @return result node
     */
    public static <T> ResultNode<T> empty() {
        return new ResultNode<>(null, null);
    }

    /**
     * get payload.
     *
     * @return payload
     */
    public final T getPayload() {
        return payload;
    }

    /**
     * get children result node.
     *
     * @return list result node
     */
    public final List<ResultNode<T>> getChildren() {
        return children;
    }

    /**
     * is empty.
     *
     * @return true if payload is null
     */
    public final boolean isEmpty() {
        return payload == null;
    }

    @Override
    public String toString() {
        return toString(" ");
    }

    /**
     * format string for check.
     *
     * @param prefix prefix
     * @return string
     */
    protected String toString(String prefix) {
        final StringBuilder sb = new StringBuilder(prefix);
        sb.append(payload);
        sb.append(System.lineSeparator());
        if (children != null) {
            children.forEach(child -> sb.append(child.toString(prefix + prefix)));
        }
        return sb.toString();
    }

    /**
     * set new parent.
     *
     * @param parent parent
     */
    private void setParent(ResultNode<T> parent) {
        this.parent = parent;
    }
}
