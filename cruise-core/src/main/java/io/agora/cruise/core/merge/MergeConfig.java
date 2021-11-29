package io.agora.cruise.core.merge;

import io.agora.cruise.core.merge.rule.MergeRule;
import org.apache.calcite.rel.RelNode;

/** MergeConfig. */
public abstract class MergeConfig<F extends RelNode, T extends RelNode> {

    protected final Class<F> fromRelNodeType;
    protected final Class<T> toRelNodeType;

    public MergeConfig(Class<F> fromRelNodeType, Class<T> toRelNodeType) {
        this.fromRelNodeType = fromRelNodeType;
        this.toRelNodeType = toRelNodeType;
    }

    public Class<F> fromRelNodeType() {
        return fromRelNodeType;
    }

    public Class<T> toRelNodeType() {
        return toRelNodeType;
    }

    public abstract MergeRule<F, T> toMergeRule();
}
