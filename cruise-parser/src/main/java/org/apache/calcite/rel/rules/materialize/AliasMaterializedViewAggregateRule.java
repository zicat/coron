package org.apache.calcite.rel.rules.materialize;

import com.google.common.collect.*;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Objects.requireNonNull;

/** MaterializedViewAggregateRuleNew. */
public abstract class AliasMaterializedViewAggregateRule<
                C extends CustomMaterializedViewAggregateRule.Config>
        extends CustomMaterializedViewAggregateRule<C> {

    /**
     * Creates a MaterializedViewAggregateRule.
     *
     * @param config
     */
    AliasMaterializedViewAggregateRule(C config) {
        super(config);
    }

    @Override
    public Pair<@Nullable RelNode, RelNode> pushFilterToOriginalViewPlan(
            RelBuilder builder, @Nullable RelNode topViewProject, RelNode viewNode, RexNode cond) {
        // We add (and push) the filter to the view plan before triggering the rewriting.
        // This is useful in case some of the columns can be folded to same value after
        // filter is added.
        HepProgramBuilder pushFiltersProgram = new HepProgramBuilder();
        if (topViewProject != null) {
            pushFiltersProgram.addRuleInstance(config.filterProjectTransposeRule());
        }
        pushFiltersProgram
                .addRuleInstance(config.aggregateProjectPullUpConstantsRule())
                .addRuleInstance(config.projectMergeRule());
        final HepPlanner tmpPlanner = new HepPlanner(pushFiltersProgram.build());
        // Now that the planner is created, push the node
        RelNode topNode =
                builder.push(topViewProject != null ? topViewProject : viewNode)
                        .filter(cond)
                        .build();
        tmpPlanner.setRoot(topNode);
        topNode = tmpPlanner.findBestExp();
        RelNode resultTopViewProject = null;
        RelNode resultViewNode = null;
        while (topNode != null) {
            if (topNode instanceof Project) {
                if (resultTopViewProject != null) {
                    // Both projects could not be merged, we will bail out
                    return Pair.of(topViewProject, viewNode);
                }
                resultTopViewProject = topNode;
                topNode = topNode.getInput(0);
            } else if (topNode instanceof Aggregate) {
                resultViewNode = topNode;
                topNode = null;
            } else {
                // We move to the child
                topNode = topNode.getInput(0);
            }
        }
        return Pair.of(resultTopViewProject, requireNonNull(resultViewNode, "resultViewNode"));
    }

    @Override
    protected @Nullable RelNode rewriteView(
            RelBuilder relBuilder,
            RexBuilder rexBuilder,
            RexSimplify simplify,
            RelMetadataQuery mq,
            MatchModality matchModality,
            boolean unionRewriting,
            RelNode input,
            @Nullable Project topProject,
            RelNode node,
            @Nullable Project topViewProject0,
            RelNode viewNode,
            BiMap<RexTableInputRef.RelTableRef, RexTableInputRef.RelTableRef>
                    queryToViewTableMapping,
            EquivalenceClasses queryEC) {
        queryEC = new EquivalenceClasses();
        final Aggregate queryAggregate = (Aggregate) node;
        final Aggregate viewAggregate = (Aggregate) viewNode;
        // Get group by references and aggregate call input references needed
        final ImmutableBitSet.Builder indexes = ImmutableBitSet.builder();
        final ImmutableBitSet references;
        if (topProject != null && !unionRewriting) {
            // We have a Project on top, gather only what is needed
            final RelOptUtil.InputFinder inputFinder =
                    new RelOptUtil.InputFinder(new LinkedHashSet<>());
            inputFinder.visitEach(topProject.getProjects());
            references = inputFinder.build();
            for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
                indexes.set(queryAggregate.getGroupSet().nth(i));
            }
            for (int i = 0; i < queryAggregate.getAggCallList().size(); i++) {
                if (references.get(queryAggregate.getGroupCount() + i)) {
                    for (int inputIdx : queryAggregate.getAggCallList().get(i).getArgList()) {
                        indexes.set(inputIdx);
                    }
                }
            }
        } else {
            // No project on top, all of them are needed
            for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
                indexes.set(queryAggregate.getGroupSet().nth(i));
            }
            for (AggregateCall queryAggCall : queryAggregate.getAggCallList()) {
                for (int inputIdx : queryAggCall.getArgList()) {
                    indexes.set(inputIdx);
                }
            }
            references = null;
        }

        // Create mapping from query columns to view columns
        final List<RexNode> rollupNodes = new ArrayList<>();
        final Multimap<Integer, Integer> m =
                generateMapping(
                        rexBuilder,
                        simplify,
                        mq,
                        queryAggregate.getInput(),
                        viewAggregate.getInput(),
                        indexes.build(),
                        queryToViewTableMapping,
                        queryEC,
                        rollupNodes);
        if (m == null) {
            // Bail out
            return null;
        }

        // We could map all expressions. Create aggregate mapping.
        @SuppressWarnings("unused")
        int viewAggregateAdditionalFieldCount = rollupNodes.size();
        int viewInputFieldCount = viewAggregate.getInput().getRowType().getFieldCount();
        int viewInputDifferenceViewFieldCount =
                viewAggregate.getRowType().getFieldCount() - viewInputFieldCount;
        int viewAggregateTotalFieldCount =
                viewAggregate.getRowType().getFieldCount() + rollupNodes.size();
        boolean forceRollup = false;
        Mapping aggregateMapping =
                Mappings.create(
                        MappingType.FUNCTION,
                        queryAggregate.getRowType().getFieldCount(),
                        viewAggregateTotalFieldCount);
        for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
            Collection<Integer> c = m.get(queryAggregate.getGroupSet().nth(i));
            for (int j : c) {
                if (j >= viewAggregate.getInput().getRowType().getFieldCount()) {
                    // This is one of the rollup columns
                    aggregateMapping.set(i, j + viewInputDifferenceViewFieldCount);
                    forceRollup = true;
                } else {
                    int targetIdx = viewAggregate.getGroupSet().indexOf(j);
                    if (targetIdx == -1) {
                        continue;
                    }
                    aggregateMapping.set(i, targetIdx);
                }
                break;
            }
            if (aggregateMapping.getTargetOpt(i) == -1) {
                // It is not part of group by, we bail out
                return null;
            }
        }
        boolean containsDistinctAgg = false;
        for (Ord<AggregateCall> ord : Ord.zip(queryAggregate.getAggCallList())) {
            if (references != null && !references.get(queryAggregate.getGroupCount() + ord.i)) {
                // Ignore
                continue;
            }
            final AggregateCall queryAggCall = ord.e;
            if (queryAggCall.filterArg >= 0) {
                // Not supported currently
                return null;
            }
            List<Integer> queryAggCallIndexes = new ArrayList<>();
            for (int aggCallIdx : queryAggCall.getArgList()) {
                queryAggCallIndexes.add(m.get(aggCallIdx).iterator().next());
            }
            for (int j = 0; j < viewAggregate.getAggCallList().size(); j++) {
                AggregateCall viewAggCall = viewAggregate.getAggCallList().get(j);
                if (queryAggCall.getAggregation().getKind()
                                != viewAggCall.getAggregation().getKind()
                        || queryAggCall.isDistinct() != viewAggCall.isDistinct()
                        || queryAggCall.getArgList().size() != viewAggCall.getArgList().size()
                        || queryAggCall.getType() != viewAggCall.getType()
                        || viewAggCall.filterArg >= 0) {
                    // Continue
                    continue;
                }
                if (!queryAggCallIndexes.equals(viewAggCall.getArgList())) {
                    // Continue
                    continue;
                }
                aggregateMapping.set(
                        queryAggregate.getGroupCount() + ord.i, viewAggregate.getGroupCount() + j);
                if (queryAggCall.isDistinct()) {
                    containsDistinctAgg = true;
                }
                break;
            }
        }

        // To simplify things, create an identity topViewProject if not present.
        final Project topViewProject =
                topViewProject0 != null
                        ? topViewProject0
                        : (Project)
                                relBuilder
                                        .push(viewNode)
                                        .project(relBuilder.fields(), ImmutableList.of(), true)
                                        .build();

        // Generate result rewriting
        final List<RexNode> additionalViewExprs = new ArrayList<>();

        // Multimap is required since a column in the materialized view's project
        // could map to multiple columns in the target query
        final ImmutableMultimap<Integer, Integer> rewritingMapping;
        relBuilder.push(input);
        // We create view expressions that will be used in a Project on top of the
        // view in case we need to rollup the expression
        final List<RexNode> inputViewExprs = new ArrayList<>(relBuilder.fields());
        if (forceRollup
                || queryAggregate.getGroupCount() != viewAggregate.getGroupCount()
                || matchModality == MatchModality.VIEW_PARTIAL) {
            if (containsDistinctAgg) {
                // Cannot rollup DISTINCT aggregate
                return null;
            }
            // Target is coarser level of aggregation. Generate an aggregate.
            final ImmutableMultimap.Builder<Integer, Integer> rewritingMappingB =
                    ImmutableMultimap.builder();
            final ImmutableBitSet.Builder groupSetB = ImmutableBitSet.builder();
            for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
                final int targetIdx = aggregateMapping.getTargetOpt(i);
                if (targetIdx == -1) {
                    // No matching group by column, we bail out
                    return null;
                }
                if (targetIdx >= viewAggregate.getRowType().getFieldCount()) {
                    RexNode targetNode =
                            rollupNodes.get(
                                    targetIdx
                                            - viewInputFieldCount
                                            - viewInputDifferenceViewFieldCount);
                    // We need to rollup this expression
                    final Multimap<RexNode, Integer> exprsLineage = ArrayListMultimap.create();
                    for (int r : RelOptUtil.InputFinder.bits(targetNode)) {
                        final int j = find(viewNode, r);
                        final int k = find(topViewProject, j);
                        if (k < 0) {
                            // No matching column needed for computed expression, bail out
                            return null;
                        }
                        final RexInputRef ref =
                                relBuilder.with(viewNode.getInput(0), b -> b.field(r));
                        exprsLineage.put(ref, k);
                    }
                    // We create the new node pointing to the index
                    groupSetB.set(inputViewExprs.size());
                    rewritingMappingB.put(inputViewExprs.size(), i);
                    additionalViewExprs.add(new RexInputRef(targetIdx, targetNode.getType()));
                    // We need to create the rollup expression
                    RexNode rollupExpression =
                            requireNonNull(
                                    shuttleReferences(rexBuilder, targetNode, exprsLineage),
                                    () ->
                                            "shuttleReferences produced null for targetNode="
                                                    + targetNode
                                                    + ", exprsLineage="
                                                    + exprsLineage);
                    inputViewExprs.add(rollupExpression);
                } else {
                    // This expression should be referenced directly
                    final int k = find(topViewProject, targetIdx);
                    if (k < 0) {
                        // No matching group by column, we bail out
                        return null;
                    }
                    groupSetB.set(k);
                    rewritingMappingB.put(k, i);
                }
            }
            final ImmutableBitSet groupSet = groupSetB.build();
            final List<RelBuilder.AggCall> aggregateCalls = new ArrayList<>();
            for (Ord<AggregateCall> ord : Ord.zip(queryAggregate.getAggCallList())) {
                final int sourceIdx = queryAggregate.getGroupCount() + ord.i;
                if (references != null && !references.get(sourceIdx)) {
                    // Ignore
                    continue;
                }
                final int targetIdx = aggregateMapping.getTargetOpt(sourceIdx);
                if (targetIdx < 0) {
                    // No matching aggregation column, we bail out
                    return null;
                }
                final int k = find(topViewProject, targetIdx);
                if (k < 0) {
                    // No matching aggregation column, we bail out
                    return null;
                }
                final AggregateCall queryAggCall = ord.e;
                SqlAggFunction rollupAgg = queryAggCall.getAggregation().getRollup();
                if (rollupAgg == null) {
                    // Cannot rollup this aggregate, bail out
                    return null;
                }
                rewritingMappingB.put(k, queryAggregate.getGroupCount() + aggregateCalls.size());
                final RexInputRef operand = rexBuilder.makeInputRef(input, k);
                aggregateCalls.add(
                        relBuilder
                                .aggregateCall(rollupAgg, operand)
                                .approximate(queryAggCall.isApproximate())
                                .distinct(queryAggCall.isDistinct())
                                .as(queryAggCall.name));
            }
            // Create aggregate on top of input
            final RelNode prevNode = relBuilder.peek();
            if (inputViewExprs.size() > prevNode.getRowType().getFieldCount()) {
                relBuilder.project(inputViewExprs);
            }
            relBuilder.aggregate(relBuilder.groupKey(groupSet), aggregateCalls);
            if (prevNode == relBuilder.peek()
                    && groupSet.cardinality() != relBuilder.peek().getRowType().getFieldCount()) {
                // Aggregate was not inserted but we need to prune columns
                relBuilder.project(relBuilder.fields(groupSet));
            }
            // We introduce a project on top, as group by columns order is lost.
            // Multimap is required since a column in the materialized view's project
            // could map to multiple columns in the target query.
            rewritingMapping = rewritingMappingB.build();
            final ImmutableMultimap<Integer, Integer> inverseMapping = rewritingMapping.inverse();
            final List<RexNode> projects = new ArrayList<>();

            final ImmutableBitSet.Builder addedProjects = ImmutableBitSet.builder();
            for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
                final int pos = groupSet.indexOf(inverseMapping.get(i).iterator().next());
                addedProjects.set(pos);
                projects.add(relBuilder.field(pos));
            }

            final ImmutableBitSet projectedCols = addedProjects.build();
            // We add aggregate functions that are present in result to projection list
            for (int i = 0; i < relBuilder.peek().getRowType().getFieldCount(); i++) {
                if (!projectedCols.get(i)) {
                    projects.add(relBuilder.field(i));
                }
            }
            relBuilder.project(projects);
        } else {
            rewritingMapping = null;
        }

        // Add query expressions on top. We first map query expressions to view
        // expressions. Once we have done that, if the expression is contained
        // and we have introduced already an operator on top of the input node,
        // we use the mapping to resolve the position of the expression in the
        // node.
        final RelDataType topRowType;
        final List<RexNode> topExprs = new ArrayList<>();
        if (topProject != null && !unionRewriting) {
            topExprs.addAll(topProject.getProjects());
            topRowType = topProject.getRowType();
        } else {
            // Add all
            for (int pos = 0; pos < queryAggregate.getRowType().getFieldCount(); pos++) {
                topExprs.add(rexBuilder.makeInputRef(queryAggregate, pos));
            }
            topRowType = queryAggregate.getRowType();
        }
        // Available in view.
        final Multimap<RexNode, Integer> viewExprs = ArrayListMultimap.create();
        addAllIndexed(viewExprs, topViewProject.getProjects());
        addAllIndexed(viewExprs, additionalViewExprs);
        final List<RexNode> rewrittenExprs = new ArrayList<>(topExprs.size());
        for (RexNode expr : topExprs) {
            // First map through the aggregate
            final RexNode e2 = shuttleReferences(rexBuilder, expr, aggregateMapping);
            if (e2 == null) {
                // Cannot map expression
                return null;
            }
            // Next map through the last project
            final RexNode e3 =
                    shuttleReferences(
                            rexBuilder, e2, viewExprs, relBuilder.peek(), rewritingMapping);
            if (e3 == null) {
                // Cannot map expression
                return null;
            }
            rewrittenExprs.add(e3);
        }
        return relBuilder
                .project(rewrittenExprs, topRowType.getFieldNames())
                .convert(topRowType, false)
                .build();
    }

    private static int find(RelNode rel, int ref) {
        if (rel instanceof Project) {
            Project project = (Project) rel;
            for (Ord<RexNode> p : Ord.zip(project.getProjects())) {
                if (p.e instanceof RexInputRef && ((RexInputRef) p.e).getIndex() == ref) {
                    return p.i;
                }
            }
        }
        if (rel instanceof Aggregate) {
            Aggregate aggregate = (Aggregate) rel;
            int k = aggregate.getGroupSet().indexOf(ref);
            if (k >= 0) {
                return k;
            }
        }
        return -1;
    }

    private static <K> void addAllIndexed(
            Multimap<K, Integer> multimap, Iterable<? extends K> list) {
        for (K k : list) {
            multimap.put(k, multimap.size());
        }
    }
}
