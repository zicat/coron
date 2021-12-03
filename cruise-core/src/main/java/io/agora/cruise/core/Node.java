package io.agora.cruise.core;

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
     * boolean is similar.
     *
     * @param otherNode other node
     * @return boolean
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
