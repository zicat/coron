package io.agora.cruise.core.merge;

import io.agora.cruise.core.merge.rule.MergeRule;

import java.util.LinkedList;
import java.util.Queue;

/** MergeConfig. */
public abstract class MergeConfig {

    protected Operand operand;
    protected boolean materialized = true;
    protected int fromLookAhead;
    protected int toLookAhead;

    /**
     * configure operands.
     *
     * @param operand operand
     * @return this of MergeConfig
     */
    public final MergeConfig withOperandSupplier(Operand operand) {
        this.operand = operand;
        this.fromLookAhead = lookAhead(operand, Operand::isAnyFromNodeType);
        this.toLookAhead = lookAhead(operand, Operand::isAnyToNodeType);
        return this;
    }

    /**
     * config materialized.
     *
     * @param materialized materialized
     * @return this
     */
    public final MergeConfig materialized(boolean materialized) {
        this.materialized = materialized;
        return this;
    }

    /**
     * compute the look ahead size by TwoMergeType.
     *
     * @param root root TwoMergeType
     * @param handler type handler
     * @return look ahead size
     */
    private static int lookAhead(Operand root, ConfigRelNodeTypeHandler handler) {
        int ahead = 0;
        final Queue<Operand> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            final Operand operand = queue.poll();
            if (operand.parent() != null) {
                queue.offer(operand.parent());
            }
            if (!handler.isAnyNodeType(operand)) {
                ahead++;
            }
        }
        return ahead;
    }

    /**
     * get materialized.
     *
     * @return boolean materialized
     */
    public boolean canMaterialized() {
        return materialized;
    }

    /**
     * get operand.
     *
     * @return operand
     */
    public final Operand operand() {
        return operand;
    }

    /**
     * cast as sub class.
     *
     * @param clazz clazz
     * @param <T> type t
     * @return sub class instance
     */
    @SuppressWarnings({"unchecked"})
    public <T> T as(Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return (T) this;
        }
        throw new RuntimeException("cast error");
    }

    /**
     * create merge rule by merge config.
     *
     * @return MergeRule
     */
    public abstract MergeRule toMergeRule();

    /** ConfigRelNodeTypeHandler. */
    private interface ConfigRelNodeTypeHandler {

        /**
         * is any node type of config.
         *
         * @param operand operand
         * @return true if is any node type
         */
        boolean isAnyNodeType(Operand operand);
    }
}
