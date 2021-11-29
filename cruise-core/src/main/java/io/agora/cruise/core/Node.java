package io.agora.cruise.core;

import java.util.*;

/** Node. */
public class Node<T> {

    protected final Node<T> parent;
    protected final List<Node<T>> children = new ArrayList<>();
    protected final T payload;
    protected final int indexInParent;

    public Node(T payload) {
        this(null, payload);
    }

    public Node(Node<T> parent, T payload) {
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
        T newPayload = payload.equals(otherNode.getPayload()) ? payload : null;
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
    public final ResultNode<T> leafMerge(Node<T> otherNode) {
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
        return parent.children.size() - getIndexInParent();
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

    /**
     * merge one right brother.
     *
     * @param otherNode other node
     * @param i i from this
     * @return result node
     */
    private ResultNode<T> rightBrotherMerge(Node<T> otherNode, int i) {

        if (otherNode.isRoot() || this.isRoot()) {
            return ResultNode.of(null);
        }
        Node<T> brother = rightBrother(i);
        Node<T> otherBrother = otherNode.rightBrother(i);
        if (brother == null
                || !brother.isLeaf()
                || otherBrother == null
                || !otherBrother.isLeaf()) {
            return ResultNode.of(null);
        }
        return brother.leafMerge(otherBrother);
    }

    /**
     * find the same sub node from rootFrom by nodeTo.
     *
     * @param rootFrom node from
     * @param nodeTo node to
     * @param <T> payload
     * @return list
     */
    public static <T> ResultNodeList<T> findSubNode(Node<T> rootFrom, Node<T> nodeTo) {

        List<Node<T>> nodeFromLeaves = findAllFirstLeafNode(rootFrom);
        List<Node<T>> nodeToLeaves = findAllFirstLeafNode(nodeTo);

        ResultNodeList<T> result = new ResultNodeList<>();
        for (Node<T> toLeaf : nodeToLeaves) {
            for (Node<T> fromLeaf : nodeFromLeaves) {
                ResultNode<T> leafResult = toLeaf.leafMerge(fromLeaf);
                if (!leafResult.isPresent()) {
                    continue;
                }
                ResultNodeList<T> resultOffset = new ResultNodeList<>(leafResult);
                Node<T> toOffset = toLeaf;
                Node<T> fromOffset = fromLeaf;
                ResultNode<T> maxResultNode = null;
                while (!toOffset.isRoot()) {
                    int size = sameSize(fromOffset.rightBrotherSize(), toOffset.rightBrotherSize());
                    for (int i = 0; i < size; i++) {
                        if (!resultOffset.add(toOffset.rightBrotherMerge(fromOffset, i))) {
                            break;
                        }
                    }
                    if (resultOffset.size() != size) {
                        break;
                    }
                    ResultNode<T> parentResultNode = toOffset.parentMerge(fromOffset, resultOffset);
                    if (!parentResultNode.isPresent()) {
                        break;
                    }
                    resultOffset = new ResultNodeList<>(parentResultNode);
                    toOffset = toOffset.parent;
                    fromOffset = fromOffset.parent;
                    maxResultNode = parentResultNode;
                }
                result.add(maxResultNode);
            }
        }
        return result;
    }

    /**
     * find all first leave nodes.
     *
     * <p>Example:
     *
     * <p>a -> b -> e
     *
     * <p>a -> b ->f
     *
     * <p>a -> c
     *
     * <p>a -> d
     *
     * <p>Got Node [e, c]
     *
     * @param node parent node
     * @param <T> payload
     * @return list
     */
    private static <T> List<Node<T>> findAllFirstLeafNode(Node<T> node) {

        List<Node<T>> result = new ArrayList<>();
        if (node == null) {
            return result;
        }

        Set<Node<T>> parent = new HashSet<>();
        Stack<Node<T>> stack = new Stack<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            Node<T> popNode = stack.pop();
            if (!popNode.isLeaf()) {
                for (int i = popNode.children.size() - 1; i >= 0; i--) {
                    stack.push(popNode.children.get(i));
                }
            } else {
                if (!popNode.isRoot() && !parent.contains(popNode.parent)) {
                    result.add(popNode);
                    parent.add(popNode.parent);
                }
            }
        }
        return result;
    }

    /**
     * checkout two size is equals.
     *
     * @param size1 size1
     * @param size2 size2
     * @return if not equals return -1 else return size
     */
    private static int sameSize(int size1, int size2) {
        return size1 == size2 ? size1 : -1;
    }
}
