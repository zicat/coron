package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

public class AliasMaterializedViewProjectAggregateRule
        extends AliasMaterializedViewAggregateRule<
                AliasMaterializedViewProjectAggregateRule.Config> {

    private AliasMaterializedViewProjectAggregateRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public AliasMaterializedViewProjectAggregateRule(
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
    public AliasMaterializedViewProjectAggregateRule(
            RelBuilderFactory relBuilderFactory,
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
        final Project project = call.rel(0);
        final Aggregate aggregate = call.rel(1);
        perform(call, project, aggregate);
    }

    /** Rule configuration. */
    public interface Config extends CustomMaterializedViewAggregateRule.Config {

        Config DEFAULT = create(RelFactories.LOGICAL_BUILDER);

        static Config create(RelBuilderFactory relBuilderFactory) {
            return CustomMaterializedViewAggregateRule.Config.create(relBuilderFactory)
                    .withGenerateUnionRewriting(true)
                    .withUnionRewritingPullProgram(null)
                    .withOperandSupplier(
                            b0 ->
                                    b0.operand(Project.class)
                                            .oneInput(
                                                    b1 -> b1.operand(Aggregate.class).anyInputs()))
                    .withDescription("CustomMaterializedViewAggregateRule(Project-Aggregate)")
                    .as(Config.class);
        }

        @Override
        default AliasMaterializedViewProjectAggregateRule toRule() {
            return new AliasMaterializedViewProjectAggregateRule(this);
        }
    }
}
