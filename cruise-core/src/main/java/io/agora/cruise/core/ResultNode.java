package io.agora.cruise.core;

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
    protected int thisLookAHead = 1;
    protected int otherLookAhead = 1;

    private ResultNode(T payload, List<ResultNode<T>> children) {
        this.payload = payload;
        this.children = children;
        if (children != null) {
            children.forEach(child -> child.setParent(this));
        }
    }

    public void setThisLookAHead(int thisLookAHead) {
        this.thisLookAHead = thisLookAHead;
    }

    public void setOtherLookAhead(int otherLookAhead) {
        this.otherLookAhead = otherLookAhead;
    }

    public static <T> ResultNode<T> of(T payload) {
        return of(payload, null);
    }

    public static <T> ResultNode<T> of(T payload, List<ResultNode<T>> children) {
        return new ResultNode<>(payload, children);
    }

    public static <T> ResultNode<T> of(List<ResultNode<T>> children) {
        return new ResultNode<>(null, children);
    }

    public final T getPayload() {
        return payload;
    }

    public final List<ResultNode<T>> getChildren() {
        return children;
    }

    public final boolean isPresent() {
        return payload != null;
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
        StringBuilder sb = new StringBuilder(prefix);
        sb.append(payload);
        sb.append(System.lineSeparator());
        if (children != null) {
            children.forEach(child -> sb.append(child.toString(prefix + prefix)));
        }
        return sb.toString();
    }

    private void setParent(ResultNode<T> parent) {
        this.parent = parent;
    }
}
