package io.agora.cruise.core.merge.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.Operand;
import io.agora.cruise.core.util.ListComparator;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AggregateMergeRule.
 *
 * <p>AggregateMergeCondition:
 *
 * <p>1. the groupSet of two Aggregations mapping to the same field of newInput
 *
 * <p>2. all aggregation functions of two Aggregation return the differName or defaultName or
 * (sameName and sameFunction)
 *
 * <p>3. The preInput of two Aggregations is equal OR the inputRef in the filter of preInput must in
 * group set.
 *
 * <p>OutputRowTypeFields: from GroupSet + from Aggregation Function Name + to Aggregation Function
 * Name.
 */
public class AggregateMergeRule extends MergeRule {

    public AggregateMergeRule(Config mergeConfig) {
        super(mergeConfig);
    }

    @Override
    public RelNode merge(
            Node<RelNode> fromNode,
            Node<RelNode> toNode,
            ResultNodeList<RelNode> childrenResultNode) {

        // aggregate only has one input
        if (childrenResultNode.size() != 1) {
            return null;
        }

        final Aggregate fromAggregate = (Aggregate) fromNode.getPayload();
        final Aggregate toAggregate = (Aggregate) toNode.getPayload();
        final RelNode newInput = childrenResultNode.get(0).getPayload();
        if (!checkAggregateMergeable(fromAggregate, toAggregate, newInput)
                || callOutputNameEqual(fromAggregate, toAggregate, newInput)) {
            return null;
        }

        final Pair<ImmutableBitSet, ImmutableList<ImmutableBitSet>> newGroupSetTuple =
                createNewGroupSet(fromAggregate, toAggregate, newInput);
        if (newGroupSetTuple == null) {
            return null;
        }

        final List<AggregateCall> newAggCalls =
                createNewAggregateCalls(fromAggregate, toAggregate, newInput);
        if (newAggCalls == null) {
            return null;
        }
        final RelTraitSet newRelTraitSet =
                fromAggregate.getTraitSet().merge(toAggregate.getTraitSet());
        return fromAggregate.copy(
                newRelTraitSet,
                newInput,
                newGroupSetTuple.left,
                newGroupSetTuple.right,
                newAggCalls);
    }

    /**
     * check aggregate mergeable.
     *
     * <p>check all identifies in filter condition must also in group set and select list.
     *
     * @param fromAggregate aggregate
     * @param toAggregate toAggregate
     * @param newInput newInput
     * @return boolean mergeable
     */
    private boolean checkAggregateMergeable(
            Aggregate fromAggregate, Aggregate toAggregate, RelNode newInput) {
        final List<Filter> fromFilters = TopFilterFinder.find(fromAggregate);
        final List<Filter> toFilters = TopFilterFinder.find(toAggregate);
        final List<Filter> newFilters = TopFilterFinder.find(newInput);
        if (fromFilters.size() != toFilters.size() || fromFilters.size() != newFilters.size()) {
            return AggregateMergeableCheck.mergeable(fromAggregate)
                    && AggregateMergeableCheck.mergeable(toAggregate);
        }

        for (int i = 0; i < fromFilters.size(); i++) {
            Filter fromFilter = fromFilters.get(i);
            Filter toFilter = toFilters.get(i);
            Filter newFilter = newFilters.get(i);
            final Map<String, Integer> fieldIndexMapping =
                    dataTypeNameIndex(newFilter.getInput().getRowType());
            final RexNode newFromCondition =
                    createNewInputRexNode(
                            fromFilter.getCondition(), fromFilter.getInput(), fieldIndexMapping);
            final RexNode newToCondition =
                    createNewInputRexNode(
                            toFilter.getCondition(), toFilter.getInput(), fieldIndexMapping);
            if (!newFromCondition.equals(newToCondition)
                    && (!AggregateMergeableCheck.mergeable(fromAggregate)
                            || !AggregateMergeableCheck.mergeable(toAggregate))) {
                return false;
            }
        }
        return true;
    }

    /**
     * create new aggregate call.
     *
     * @param call old call
     * @param parentAggregate parent aggregate
     * @param newInput parent aggregate input
     * @return new aggregate
     */
    private static AggregateCall newAggregateCall(
            AggregateCall call, Aggregate parentAggregate, RelNode newInput) {

        final List<Integer> newArgIds =
                reMappingBitSet(call.getArgList(), parentAggregate.getInput(), newInput);
        if (newArgIds == null) {
            return null;
        }
        final Map<String, Integer> dataMapping = dataTypeNameIndex(newInput.getRowType());
        final int newFilterArg =
                call.filterArg < 0
                        ? call.filterArg
                        : dataMapping.get(
                                parentAggregate.getRowType().getFieldNames().get(call.filterArg));
        ImmutableBitSet newDistinctKey = null;
        if (call.distinctKeys != null) {
            ImmutableBitSet mapping =
                    reMappingBitSet(call.distinctKeys, parentAggregate.getInput(), newInput);
            if (mapping == null) {
                return null;
            }
            newDistinctKey = mapping;
        }
        return AggregateCall.create(
                call.getAggregation(),
                call.isDistinct(),
                call.isApproximate(),
                call.ignoreNulls(),
                newArgIds,
                newFilterArg,
                newDistinctKey,
                call.collation,
                call.type,
                call.name);
    }

    /**
     * create new aggregate call by two aggregate and input.
     *
     * @param fromAggregate from aggregation
     * @param toAggregate to aggregation
     * @param newInput newInput
     * @return null if not found else return list aggregate call
     */
    private static List<AggregateCall> createNewAggregateCalls(
            Aggregate fromAggregate, Aggregate toAggregate, RelNode newInput) {

        final Map<String, AggregateCall> newFromAggCall =
                createAllAggregateCalls(fromAggregate, newInput);
        if (newFromAggCall == null) {
            return null;
        }

        final Map<String, AggregateCall> newToAggCall =
                createAllAggregateCalls(toAggregate, newInput);
        if (newToAggCall == null) {
            return null;
        }

        final List<AggregateCall> newAggCalls = new ArrayList<>();
        newFromAggCall.forEach((k, v) -> newAggCalls.add(v));
        for (Map.Entry<String, AggregateCall> entry : newToAggCall.entrySet()) {
            String name = entry.getKey();
            AggregateCall call = entry.getValue();
            if (defaultAggregateFunctionName(name) || !newFromAggCall.containsKey(name)) {
                newAggCalls.add(call);
            } else if (!newAggCalls.contains(call)) {
                return null;
            }
        }
        return newAggCalls;
    }

    /**
     * check function name is default.
     *
     * @param name name
     * @return true if default
     */
    private static boolean defaultAggregateFunctionName(String name) {
        return name.startsWith("EXPR$");
    }

    /**
     * create all new aggregate calls and it's return names as key by new input.
     *
     * @param aggregate aggregate
     * @param input input
     * @return map
     */
    private static Map<String, AggregateCall> createAllAggregateCalls(
            Aggregate aggregate, RelNode input) {
        final List<String> fromGroupFields = groupSetNames(aggregate);
        final Map<String, AggregateCall> fromNameCallMapping =
                Maps.newLinkedHashMapWithExpectedSize(aggregate.getRowType().getFieldCount());
        for (int i = 0; i < aggregate.getRowType().getFieldCount(); i++) {
            String aggregationName = aggregate.getRowType().getFieldNames().get(i);
            if (fromGroupFields.contains(aggregationName)) {
                continue;
            }
            final AggregateCall call = aggregate.getAggCallList().get(i - fromGroupFields.size());
            final AggregateCall newCall = newAggregateCall(call, aggregate, input);
            if (newCall == null) {
                return null;
            }
            fromNameCallMapping.put(aggregationName, newCall);
        }
        return fromNameCallMapping;
    }

    /**
     * check from fields is equal with to fields, and found it's id in input.
     *
     * @param fromFields from fields
     * @param toFields to fields
     * @param relDataType RelDataType
     * @return if not equals return null else return index list
     */
    private static List<Integer> sameIndexMapping(
            List<String> fromFields, List<String> toFields, RelDataType relDataType) {

        if (fromFields == null || toFields == null) {
            return null;
        }

        if (fromFields.size() != toFields.size()) {
            return null;
        }

        final List<Integer> newGroupSet = new ArrayList<>(fromFields.size());
        final Map<String, Integer> indexMapping = dataTypeNameIndex(relDataType);
        for (String field : fromFields) {
            if (!toFields.contains(field)) {
                return null;
            }
            Integer offset = indexMapping.get(field);
            if (offset == null) {
                return null;
            }
            newGroupSet.add(offset);
        }
        newGroupSet.sort(Integer::compareTo);
        return newGroupSet;
    }

    /**
     * create new group set.
     *
     * @param fromAggregate from aggregate
     * @param toAggregate to aggregate
     * @param input same input
     * @return null if create fail else ImmutableBitSet
     */
    private Pair<ImmutableBitSet, ImmutableList<ImmutableBitSet>> createNewGroupSet(
            Aggregate fromAggregate, Aggregate toAggregate, RelNode input) {

        // calcite not support materialized group sets, so merged group sets is meaningless
        if (mergeConfig.canMaterialized()
                && (fromAggregate.getGroupType() != Aggregate.Group.SIMPLE
                        || toAggregate.getGroupType() != Aggregate.Group.SIMPLE)) {
            return null;
        }

        if (fromAggregate.getGroupSets().size() != toAggregate.getGroupSets().size()) {
            return null;
        }

        final List<Integer> newGroupSet =
                sameIndexMapping(
                        groupSetNames(fromAggregate),
                        groupSetNames(toAggregate),
                        input.getRowType());
        if (newGroupSet == null) {
            return null;
        }

        final List<List<String>> fromField = getNewSortedGroupField(fromAggregate);
        final List<List<String>> toField = getNewSortedGroupField(toAggregate);
        if (fromField == null || toField == null) {
            return null;
        }

        final List<ImmutableBitSet> newGroupSets = new ArrayList<>(fromField.size());
        for (int i = 0; i < fromField.size(); i++) {
            final List<String> groupValueFieldFrom = fromField.get(i);
            final List<String> groupValueFieldTo = toField.get(i);
            if (!groupValueFieldFrom.equals(groupValueFieldTo)) {
                return null;
            }
            final List<Integer> newGroupValue =
                    sameIndexMapping(groupValueFieldFrom, groupValueFieldTo, input.getRowType());
            newGroupSets.add(ImmutableBitSet.of(newGroupValue));
        }
        return Pair.of(ImmutableBitSet.of(newGroupSet), ImmutableList.copyOf(newGroupSets));
    }

    /**
     * get aggregate fields.
     *
     * @param aggregate aggregate
     * @return list list field
     */
    private static List<List<String>> getNewSortedGroupField(Aggregate aggregate) {
        final List<List<String>> toField = new ArrayList<>(aggregate.getGroupSets().size());
        for (ImmutableBitSet bitSet : aggregate.getGroupSets()) {
            final List<String> fields =
                    createNameByBitSetType(bitSet, aggregate.getInput().getRowType());
            if (fields == null) {
                return null;
            }
            toField.add(fields);
        }
        toField.sort(new ListComparator<>());
        return toField;
    }

    /**
     * check function output name is same in from agg and to agg.
     *
     * @param fromAggregate fromAggregate
     * @param toAggregate toAggregate
     * @param input newInput
     * @return boolean contains equal
     */
    private boolean callOutputNameEqual(
            Aggregate fromAggregate, Aggregate toAggregate, RelNode input) {
        final Map<String, AggregateCall> fromNameCallMapping =
                createAllAggregateCalls(fromAggregate, input);

        final Map<String, AggregateCall> toNameCallMapping =
                createAllAggregateCalls(toAggregate, input);

        if (fromNameCallMapping == null || toNameCallMapping == null) {
            return true;
        }

        for (Map.Entry<String, AggregateCall> entry : fromNameCallMapping.entrySet()) {
            final String groupName = entry.getKey();
            if (defaultAggregateFunctionName(groupName)) {
                continue;
            }
            final AggregateCall fromCall = entry.getValue();
            final AggregateCall toCall = toNameCallMapping.get(groupName);
            if (toCall == null) {
                continue;
            }
            // if fromCall name equals toCall name, but function not equals, replace fail
            // <example> from: select sum(p) as a ; to: select max(p) as a
            // <reason> because all merge logic base on name, parent get name from it's input and
            // try to found the index in merge input by name, but merge input has multi
            // RexNode(sump(p),max(p)), and fail to select which is actually need
            if (!fromCall.equals(toCall)) {
                return true;
            }
        }
        return false;
    }

    /**
     * create GroupSet (name,id) mapping.
     *
     * @param aggregate aggregating
     * @return mapping
     */
    private static List<String> groupSetNames(Aggregate aggregate) {
        return createNameByBitSetType(aggregate.getGroupSet(), aggregate.getInput().getRowType());
    }

    /**
     * @param bitSet bit set
     * @param relDataType RelDataType
     * @return list names.
     */
    private static List<String> createNameByBitSetType(
            ImmutableBitSet bitSet, RelDataType relDataType) {
        final List<String> result = new ArrayList<>(bitSet.size());
        for (int i = 0; i < bitSet.size(); i++) {
            if (!bitSet.get(i)) {
                continue;
            }
            if (relDataType.getFieldNames().size() > i) {
                result.add(relDataType.getFieldNames().get(i));
            } else {
                return null;
            }
        }
        result.sort(String::compareTo);
        return result;
    }

    /**
     * remapping bitset.
     *
     * @param bitSet bit set
     * @param from from
     * @param to to
     * @return bit set
     */
    private static ImmutableBitSet reMappingBitSet(
            ImmutableBitSet bitSet, RelNode from, RelNode to) {
        if (bitSet == null) {
            return null;
        }
        final List<String> fields = createNameByBitSetType(bitSet, from.getRowType());
        if (fields == null) {
            return null;
        }
        final List<Integer> result = new ArrayList<>(fields.size());
        final Map<String, Integer> dataMapping = dataTypeNameIndex(to.getRowType());
        for (String field : fields) {
            Integer index = dataMapping.get(field);
            if (index == null) {
                return null;
            }
            result.add(index);
        }
        result.sort(Integer::compareTo);
        return ImmutableBitSet.of(result);
    }

    /**
     * remapping bitset.
     *
     * @param bitSet bit set
     * @param from from
     * @param to to
     * @return bit set
     */
    private static List<Integer> reMappingBitSet(List<Integer> bitSet, RelNode from, RelNode to) {
        return reMappingBitSet(ImmutableBitSet.of(bitSet), from, to).toList();
    }

    /** aggregate config. */
    public static class Config extends MergeConfig {

        public static Config create() {
            return new Config()
                    .withOperandSupplier(Operand.of(Aggregate.class, Aggregate.class))
                    .as(Config.class);
        }

        @Override
        public AggregateMergeRule toMergeRule() {
            return new AggregateMergeRule(this);
        }
    }
}
