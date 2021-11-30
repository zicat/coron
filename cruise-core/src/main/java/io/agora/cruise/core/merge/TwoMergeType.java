package io.agora.cruise.core.merge;

/** TwoMergeType. */
public class TwoMergeType {

    protected final Class<?> fromRelNodeType;
    protected final Class<?> toRelNodeType;
    protected final TwoMergeType parent;

    public TwoMergeType(Class<?> fromRelNodeType, Class<?> toRelNodeType, TwoMergeType parent) {
        this.fromRelNodeType = fromRelNodeType;
        this.toRelNodeType = toRelNodeType;
        this.parent = parent;
    }

    public TwoMergeType(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        this(fromRelNodeType, toRelNodeType, null);
    }

    public final Class<?> fromRelNodeType() {
        return fromRelNodeType;
    }

    public final TwoMergeType getParent() {
        return parent;
    }

    public final Class<?> toRelNodeType() {
        return toRelNodeType;
    }
}
