package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

public class AliasMaterializedViewOnlyJoinRule
        extends AliasMaterializedViewJoinRule<MaterializedViewJoinRule.Config> {

    AliasMaterializedViewOnlyJoinRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public AliasMaterializedViewOnlyJoinRule(
            RelBuilderFactory relBuilderFactory,
            boolean generateUnionRewriting,
            HepProgram unionRewritingPullProgram,
            boolean fastBailOut) {
        this(
                Config.DEFAULT
                        .withGenerateUnionRewriting(generateUnionRewriting)
                        .withUnionRewritingPullProgram(unionRewritingPullProgram)
                        .withFastBailOut(fastBailOut)
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(Config.class));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        perform(call, null, join);
    }

    /** Rule configuration. */
    public interface Config extends MaterializedViewJoinRule.Config {
        Config DEFAULT =
                EMPTY.withOperandSupplier(b -> b.operand(Join.class).anyInputs())
                        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .withDescription("MaterializedViewJoinRule(Join)")
                        .as(Config.class)
                        .withGenerateUnionRewriting(true)
                        .withUnionRewritingPullProgram(null)
                        .withFastBailOut(true)
                        .as(Config.class);

        @Override
        default AliasMaterializedViewOnlyJoinRule toRule() {
            return new AliasMaterializedViewOnlyJoinRule(this);
        }
    }
}
