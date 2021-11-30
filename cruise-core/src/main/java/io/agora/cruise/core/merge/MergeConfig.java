package io.agora.cruise.core.merge;

import io.agora.cruise.core.merge.rule.MergeRule;

/** MergeConfig. */
public abstract class MergeConfig extends TwoMergeType {

    public MergeConfig(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        this(fromRelNodeType, toRelNodeType, null);
    }

    public MergeConfig(Class<?> fromRelNodeType, Class<?> toRelNodeType, TwoMergeType parent) {
        super(fromRelNodeType, toRelNodeType, parent);
    }

    /**
     * create merge rule by merge config.
     *
     * @return MergeRule
     */
    public abstract MergeRule toMergeRule();
}
