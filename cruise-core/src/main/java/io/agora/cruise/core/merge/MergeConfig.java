package io.agora.cruise.core.merge;

import io.agora.cruise.core.merge.rule.MergeRule;

/** MergeConfig. */
public abstract class MergeConfig {

    protected Operand operand;

    /**
     * configure operands.
     *
     * @param operand operand
     * @return this of MergeConfig
     */
    public final MergeConfig withOperandSupplier(Operand operand) {
        this.operand = operand;
        return this;
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
}
