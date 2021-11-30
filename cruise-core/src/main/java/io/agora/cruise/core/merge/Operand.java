package io.agora.cruise.core.merge;

/** TwoMergeType. */
public class Operand {

    public static final Class<?> ENY_NODE_TYPE = Object.class;

    protected final Class<?> fromRelNodeType;
    protected final Class<?> toRelNodeType;
    protected Operand parent;

    public Operand(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        this.fromRelNodeType = fromRelNodeType;
        this.toRelNodeType = toRelNodeType;
    }

    public Operand operand(Operand parent) {
        this.parent = parent;
        return this;
    }

    public static Operand of(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        return new Operand(fromRelNodeType, toRelNodeType);
    }

    public final Class<?> fromRelNodeType() {
        return fromRelNodeType;
    }

    public final Operand parent() {
        return parent;
    }

    public final Class<?> toRelNodeType() {
        return toRelNodeType;
    }
}
