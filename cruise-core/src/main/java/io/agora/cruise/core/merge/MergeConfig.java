package io.agora.cruise.core.merge;

import io.agora.cruise.core.merge.rule.MergeRule;
import org.apache.calcite.rel.RelNode;

/** MergeConfig. */
public abstract class MergeConfig<F extends RelNode, T extends RelNode> extends TwoMergeType<F, T> {

    public MergeConfig(Class<F> fromRelNodeType, Class<T> toRelNodeType) {
        this(fromRelNodeType, toRelNodeType, null);
    }

    public MergeConfig(
            Class<F> fromRelNodeType, Class<T> toRelNodeType, TwoMergeType<?, ?> parent) {
        super(fromRelNodeType, toRelNodeType, parent);
    }

    public abstract MergeRule<F, T> toMergeRule();
}
