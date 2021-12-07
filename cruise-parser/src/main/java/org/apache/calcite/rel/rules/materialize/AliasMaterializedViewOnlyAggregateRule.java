package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

public class AliasMaterializedViewOnlyAggregateRule
        extends AliasMaterializedViewAggregateRule<AliasMaterializedViewOnlyAggregateRule.Config> {

    private AliasMaterializedViewOnlyAggregateRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public AliasMaterializedViewOnlyAggregateRule(
            RelBuilderFactory relBuilderFactory,
            boolean generateUnionRewriting,
            HepProgram unionRewritingPullProgram) {
        this(
                Config.create(relBuilderFactory)
                        .withGenerateUnionRewriting(generateUnionRewriting)
                        .withUnionRewritingPullProgram(unionRewritingPullProgram)
                        .as(Config.class));
    }

    @Deprecated // to be removed before 2.0
    public AliasMaterializedViewOnlyAggregateRule(
            RelOptRuleOperand operand,
            RelBuilderFactory relBuilderFactory,
            String description,
            boolean generateUnionRewriting,
            HepProgram unionRewritingPullProgram,
            RelOptRule filterProjectTransposeRule,
            RelOptRule filterAggregateTransposeRule,
            RelOptRule aggregateProjectPullUpConstantsRule,
            RelOptRule projectMergeRule) {
        this(
                Config.create(relBuilderFactory)
                        .withGenerateUnionRewriting(generateUnionRewriting)
                        .withUnionRewritingPullProgram(unionRewritingPullProgram)
                        .withDescription(description)
                        .withOperandSupplier(b -> b.exactly(operand))
                        .as(Config.class)
                        .withFilterProjectTransposeRule(filterProjectTransposeRule)
                        .withFilterAggregateTransposeRule(filterAggregateTransposeRule)
                        .withAggregateProjectPullUpConstantsRule(
                                aggregateProjectPullUpConstantsRule)
                        .withProjectMergeRule(projectMergeRule)
                        .as(Config.class));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        perform(call, null, aggregate);
    }

    /** Rule configuration. */
    public interface Config extends CustomMaterializedViewAggregateRule.Config {
        Config DEFAULT = create(RelFactories.LOGICAL_BUILDER);

        static Config create(RelBuilderFactory relBuilderFactory) {
            return CustomMaterializedViewAggregateRule.Config.create(relBuilderFactory)
                    .withOperandSupplier(b -> b.operand(Aggregate.class).anyInputs())
                    .withDescription("CustomMaterializedViewAggregateRule(Aggregate)")
                    .as(Config.class)
                    .withGenerateUnionRewriting(true)
                    .withUnionRewritingPullProgram(null)
                    .withFastBailOut(false)
                    .as(Config.class);
        }

        @Override
        default AliasMaterializedViewOnlyAggregateRule toRule() {
            return new AliasMaterializedViewOnlyAggregateRule(this);
        }
    }
}
