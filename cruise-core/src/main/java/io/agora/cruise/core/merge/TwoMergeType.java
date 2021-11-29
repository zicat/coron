package io.agora.cruise.core.merge;

import org.apache.calcite.rel.RelNode;

/** TwoMergeType. */
public class TwoMergeType<F extends RelNode, T extends RelNode> {

    protected final Class<F> fromRelNodeType;
    protected final Class<T> toRelNodeType;
    protected final TwoMergeType<?, ?> parent;

    public TwoMergeType(
            Class<F> fromRelNodeType, Class<T> toRelNodeType, TwoMergeType<?, ?> parent) {
        this.fromRelNodeType = fromRelNodeType;
        this.toRelNodeType = toRelNodeType;
        this.parent = parent;
    }

    public Class<F> fromRelNodeType() {
        return fromRelNodeType;
    }

    public TwoMergeType<?, ?> getParent() {
        return parent;
    }

    public Class<T> toRelNodeType() {
        return toRelNodeType;
    }
}
