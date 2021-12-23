package io.agora.cruise.analyzer.shuttle;

import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.PredictRexShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** PartitionSimplify. */
public class PartitionRelShuttle extends RelShuttleImpl {

    private static final String PREFIX_NAME = "tmp_p_";
    private static final List<SqlKind> COMPARE_KIND =
            Arrays.asList(
                    SqlKind.EQUALS,
                    SqlKind.BETWEEN,
                    SqlKind.LESS_THAN,
                    SqlKind.LESS_THAN_OR_EQUAL,
                    SqlKind.GREATER_THAN,
                    SqlKind.GREATER_THAN_OR_EQUAL,
                    SqlKind.IN);

    protected final RexBuilder rexBuilder = defaultRexBuilder();
    protected final List<String> partitionFields;

    protected PartitionRelShuttle(List<String> partitionFields) {
        this.partitionFields = partitionFields;
    }

    /**
     * create partition shuttles combo.
     *
     * @param partitionFields partitionFields
     * @return RelShuttleImpl[]
     */
    public static RelShuttleImpl[] partitionShuttles(List<String> partitionFields) {
        return new RelShuttleImpl[] {
            new PartitionProjectFilterRelShuttle(partitionFields),
            new PartitionAggregateProjectRelShuttle(partitionFields)
        };
    }

    protected RexBuilder defaultRexBuilder() {
        return new RexBuilder(defaultTypeFactory());
    }

    protected SqlTypeFactoryImpl defaultTypeFactory() {
        return CalciteContext.DEFAULT_SQL_TYPE_FACTORY;
    }

    /**
     * get prefix name,default tmp_p_ .
     *
     * @return prefix name
     */
    protected String getPrefixName() {
        return PREFIX_NAME;
    }

    /**
     * get filter rex node.
     *
     * @param filter filter
     * @return tuple2
     */
    protected Tuple2<RelNode, List<RexNode>> transFilterCondition(Filter filter) {

        final RexNode rexNode = filter.getCondition();
        final RexNode leftExp = leftExpParser(rexNode, filter);
        if (leftExp != null) {
            return new Tuple2<>(filter.getInput(), Collections.singletonList(leftExp));
        }
        if (rexNode.getKind() != SqlKind.AND) {
            return null;
        }
        final RexCall andRexCall = (RexCall) rexNode;
        final List<RexNode> noPartitionRexNode = new ArrayList<>();
        final List<RexNode> partitionRexNode = new ArrayList<>();
        for (int i = 0; i < andRexCall.getOperands().size(); i++) {
            final RexNode childNode = andRexCall.getOperands().get(i);
            final RexNode childLeftExp = leftExpParser(andRexCall.getOperands().get(i), filter);
            if (childLeftExp == null) {
                noPartitionRexNode.add(childNode);
            } else if (!partitionRexNode.contains(childLeftExp)) {
                partitionRexNode.add(childLeftExp);
            }
        }
        if (partitionRexNode.isEmpty()) {
            return null;
        }
        if (noPartitionRexNode.isEmpty()) {
            return new Tuple2<>(filter.getInput(), partitionRexNode);
        }
        final RexNode newCondition =
                noPartitionRexNode.size() == 1
                        ? noPartitionRexNode.get(0)
                        : andRexCall.clone(andRexCall.type, noPartitionRexNode);
        return new Tuple2<>(
                filter.copy(filter.getTraitSet(), filter.getInput(), newCondition),
                partitionRexNode);
    }

    /**
     * containsPartitionField.
     *
     * @param rexNode rexNode
     * @param filter filter
     * @return boolean
     */
    private RexNode leftExpParser(RexNode rexNode, Filter filter) {

        if (!COMPARE_KIND.contains(rexNode.getKind())) {
            return null;
        }
        final RexCall functionCall = (RexCall) rexNode;
        final RexNode leftExp = functionCall.getOperands().get(0);
        final RelDataType relDataType = filter.getInput().getRowType();
        boolean rightNotContain = true;
        for (int i = 1; i < functionCall.getOperands().size(); i++) {
            final RexNode rightRexNode = functionCall.getOperands().get(i);
            if (!PredictRexShuttle.predicts(rightRexNode).isEmpty()) {
                rightNotContain = false;
                break;
            }
        }
        return PartitionFieldFounder.contains(leftExp, relDataType, partitionFields)
                        && rightNotContain
                ? leftExp
                : null;
    }

    /** PartitionFieldFounder. */
    private static class PartitionFieldFounder extends RexShuttle {

        private final RelDataType relDataType;
        private final List<String> partitionFields;
        private final AtomicBoolean contains = new AtomicBoolean(false);

        public PartitionFieldFounder(RelDataType relDataType, List<String> partitionFields) {
            this.relDataType = relDataType;
            this.partitionFields = partitionFields;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (partitionFields.contains(relDataType.getFieldNames().get(inputRef.getIndex()))) {
                contains.set(true);
            }
            return inputRef;
        }

        /**
         * check rexNode contains partition fields.
         *
         * @param rexNode reNode
         * @param relDataType relDataType
         * @param partitionFields partitionFields
         * @return true if contains
         */
        public static boolean contains(
                RexNode rexNode, RelDataType relDataType, List<String> partitionFields) {

            final PartitionFieldFounder fieldFounder =
                    new PartitionFieldFounder(relDataType, partitionFields);
            rexNode.accept(fieldFounder);
            return fieldFounder.contains.get();
        }
    }
}
