package io.agora.cruise.core.merge;

/** TwoMergeType. */
public class Operand {

    private static final Class<?> ENY_NODE_TYPE = Object.class;

    protected final Class<?> fromRelNodeType;
    protected final Class<?> toRelNodeType;
    protected Operand parent;

    private Operand(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        this.fromRelNodeType = fromRelNodeType;
        this.toRelNodeType = toRelNodeType;
    }

    public Operand operand(Operand parent) {
        this.parent = parent;
        return this;
    }

    /**
     * create Operand by fromRelNodeType and toRelNodeType.
     *
     * @param fromRelNodeType fromRelNodeType
     * @param toRelNodeType toRelNodeType
     * @return Operand
     */
    public static Operand of(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        return new Operand(fromRelNodeType, toRelNodeType);
    }

    /**
     * create operand by fromRelNodeType, toRelNodeType = ENY_NODE_TYPE.
     *
     * @param fromRelNodeType fromRelNodeType
     * @return Operand
     */
    public static Operand ofFrom(Class<?> fromRelNodeType) {
        return new Operand(fromRelNodeType, ENY_NODE_TYPE);
    }

    /**
     * create operand by toRelNodeType, fromRelNodeType = ENY_NODE_TYPE.
     *
     * @param toRelNodeType fromRelNodeType
     * @return Operand
     */
    public static Operand ofTo(Class<?> toRelNodeType) {
        return new Operand(ENY_NODE_TYPE, toRelNodeType);
    }

    /**
     * from rel node type is any.
     *
     * @return true if any from node type
     */
    public final boolean isAnyFromNodeType() {
        return fromRelNodeType == ENY_NODE_TYPE;
    }

    /**
     * to rel node type is any.
     *
     * @return true if any to node type
     */
    public final boolean isAnyToNodeType() {
        return toRelNodeType == ENY_NODE_TYPE;
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
