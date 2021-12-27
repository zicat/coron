package org.apache.calcite.rel.core;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;

/** AggregateCallWrapper. */
public class AggregateCallWrapper extends AggregateCall {

    private static final Field aggFunctionField;
    private static final Field distinctField;
    private static final Field approximateField;
    private static final Field ignoreNullsField;
    private static final Field typeField;
    private static final Field nameField;
    private static final Field argListField;
    private static final Field filterArgField;
    private static final Field distinctKeysField;
    private static final Field collationField;

    static {
        try {
            aggFunctionField = AggregateCall.class.getDeclaredField("aggFunction");
            aggFunctionField.setAccessible(true);
            distinctField = AggregateCall.class.getDeclaredField("distinct");
            distinctField.setAccessible(true);
            approximateField = AggregateCall.class.getDeclaredField("approximate");
            approximateField.setAccessible(true);
            ignoreNullsField = AggregateCall.class.getDeclaredField("ignoreNulls");
            ignoreNullsField.setAccessible(true);
            typeField = AggregateCall.class.getField("type");
            typeField.setAccessible(true);
            nameField = AggregateCall.class.getField("name");
            nameField.setAccessible(true);
            argListField = AggregateCall.class.getDeclaredField("argList");
            argListField.setAccessible(true);
            filterArgField = AggregateCall.class.getField("filterArg");
            filterArgField.setAccessible(true);
            distinctKeysField = AggregateCall.class.getField("distinctKeys");
            distinctKeysField.setAccessible(true);
            collationField = AggregateCall.class.getField("collation");
            collationField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates an AggregateCall.
     *
     * @param aggFunction Aggregate function
     * @param distinct Whether distinct
     * @param argList List of ordinals of arguments
     * @param type Result type
     * @param name Name (may be null)
     */
    public AggregateCallWrapper(
            SqlAggFunction aggFunction,
            Boolean distinct,
            List<Integer> argList,
            RelDataType type,
            String name) {
        super(aggFunction, distinct, argList, type, name);
    }

    /**
     * create AggregateCallWrapper.
     *
     * @param aggFunction aggFunction
     * @param distinct distinct
     * @param approximate approximate
     * @param ignoreNulls ignoreNulls
     * @param argList argList
     * @param filterArg filterArg
     * @param distinctKeys distinctKeys
     * @param collation collation
     * @param type type
     * @param name name
     * @return AggregateCall
     * @throws Exception Exception
     */
    public static AggregateCall createWrapper(
            SqlAggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            boolean ignoreNulls,
            List<Integer> argList,
            int filterArg,
            @Nullable ImmutableBitSet distinctKeys,
            RelCollation collation,
            RelDataType type,
            @Nullable String name)
            throws Exception {
        final boolean distinct2 =
                distinct && (aggFunction.getDistinctOptionality() != Optionality.IGNORED);

        Constructor<AggregateCallWrapper> constructor =
                AggregateCallWrapper.class.getConstructor(
                        SqlAggFunction.class,
                        Boolean.class,
                        List.class,
                        RelDataType.class,
                        String.class);
        AggregateCallWrapper wrapper =
                constructor.newInstance(aggFunction, distinct2, argList, type, name);
        aggFunctionField.set(wrapper, aggFunction);
        distinctField.set(wrapper, distinct2);
        approximateField.set(wrapper, approximate);
        ignoreNullsField.set(wrapper, ignoreNulls);
        argListField.set(wrapper, argList);
        filterArgField.set(wrapper, filterArg);
        distinctKeysField.set(wrapper, distinctKeys);
        collationField.set(wrapper, collation);
        typeField.set(wrapper, type);
        nameField.set(wrapper, name);
        return wrapper;
    }

    /**
     * create AggregateCallWrapper.
     *
     * @param aggregateCall aggregateCall
     * @param newArgList newArgList
     * @return aggregateCall
     * @throws Exception Exception
     */
    public static AggregateCall createWrapper(AggregateCall aggregateCall, List<Integer> newArgList)
            throws Exception {
        return createWrapper(
                aggregateCall.getAggregation(),
                aggregateCall.isDistinct(),
                aggregateCall.isApproximate(),
                aggregateCall.ignoreNulls(),
                ImmutableList.copyOf(newArgList),
                aggregateCall.filterArg,
                aggregateCall.distinctKeys,
                aggregateCall.collation,
                aggregateCall.type,
                aggregateCall.name);
    }

    public Aggregate.AggCallBinding createBinding(Aggregate aggregateRelBase) {
        final RelDataType rowType = aggregateRelBase.getInput().getRowType();

        return new AggCallBinding(
                aggregateRelBase.getCluster().getTypeFactory(),
                getAggregation(),
                SqlTypeUtil.projectTypes(rowType, getArgList()),
                0,
                hasFilter());
    }

    public static class AggCallBinding extends Aggregate.AggCallBinding {

        /**
         * Creates an AggCallBinding.
         *
         * @param typeFactory Type factory
         * @param aggFunction Aggregate function
         * @param operands Data types of operands
         * @param groupCount Number of columns in the GROUP BY clause
         * @param filter Whether the aggregate function has a FILTER clause
         */
        public AggCallBinding(
                RelDataTypeFactory typeFactory,
                SqlAggFunction aggFunction,
                List<RelDataType> operands,
                int groupCount,
                boolean filter) {
            super(typeFactory, aggFunction, operands, groupCount, filter);
        }
    }
}
