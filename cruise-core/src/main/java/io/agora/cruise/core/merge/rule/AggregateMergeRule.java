package io.agora.cruise.core.merge.rule;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.core.merge.MergeConfig;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** AggregateMergeRule. */
public class AggregateMergeRule extends MergeRule<Aggregate, Aggregate> {

    public AggregateMergeRule(AggregateMergeRule.Config mergeConfig) {
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

        // create new group set
        final RelNode newInput = childrenResultNode.get(0).getPayload();
        final ImmutableBitSet newGroupSet = createNewGroupSet(fromAggregate, toAggregate, newInput);
        if (newGroupSet == null) {
            return null;
        }

        final List<AggregateCall> newAggCalls =
                createNewAggregateCalls(fromAggregate, toAggregate, newInput);
        final RelTraitSet newRelTraitSet =
                fromAggregate.getTraitSet().merge(toAggregate.getTraitSet());

        return copy(
                fromAggregate.copy(newRelTraitSet, newInput, newGroupSet, null, newAggCalls),
                childrenResultNode);
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
        final int newFilterArg =
                call.filterArg < 0
                        ? call.filterArg
                        : findIdByName(
                                parentAggregate.getRowType().getFieldNames().get(call.filterArg),
                                newInput.getRowType());

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
        newToAggCall.forEach(
                (k, v) -> {
                    if (!newAggCalls.contains(v)) {
                        newAggCalls.add(v);
                    }
                });
        return newAggCalls;
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
        final Map<String, AggregateCall> fromNameCallMapping = new LinkedHashMap<>();
        for (int i = 0; i < aggregate.getRowType().getFieldNames().size(); i++) {
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

        final List<Integer> newGroupSet = new ArrayList<>();
        for (String field : fromFields) {
            if (!toFields.contains(field)) {
                return null;
            }
            int offset = findIdByName(field, relDataType);
            if (offset == -1) {
                return null;
            }
            newGroupSet.add(offset);
        }
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
    private static ImmutableBitSet createNewGroupSet(
            Aggregate fromAggregate, Aggregate toAggregate, RelNode input) {

        if (differImmutableList(
                        fromAggregate.getGroupSets(), ImmutableList.of(fromAggregate.getGroupSet()))
                || differImmutableList(
                        toAggregate.getGroupSets(), ImmutableList.of(toAggregate.getGroupSet()))) {
            return null;
        }

        final Map<String, AggregateCall> fromNameCallMapping =
                createAllAggregateCalls(fromAggregate, input);

        final Map<String, AggregateCall> toNameCallMapping =
                createAllAggregateCalls(toAggregate, input);

        if (fromNameCallMapping == null || toNameCallMapping == null) {
            return null;
        }

        for (Map.Entry<String, AggregateCall> entry : fromNameCallMapping.entrySet()) {
            final String groupName = entry.getKey();
            final AggregateCall fromCall = entry.getValue();
            AggregateCall toCall = toNameCallMapping.get(groupName);
            if (toCall == null) {
                continue;
            }
            // if fromCall name equals toCall name, but function not equals, replace fail
            // <example> from: select sum(p) as a ; to: select max(p) as a
            // <reason> because all merge logic base on name, parent get name from it's input and
            // try to found the index in merge input by name, but merge input has multi
            // RexNode(sump(p),max(p)), and fail to select which is actually need
            if (!fromCall.equals(toCall)) {
                return null;
            }
        }

        final List<Integer> newGroupSet =
                sameIndexMapping(
                        groupSetNames(fromAggregate),
                        groupSetNames(toAggregate),
                        input.getRowType());
        return newGroupSet == null ? null : ImmutableBitSet.of(newGroupSet);
    }

    /**
     * find position from RelDataType by fieldName.
     *
     * @param fieldName fieldName
     * @param relDataType RelDataType
     * @return -1 if not found else return position
     */
    private static int findIdByName(String fieldName, RelDataType relDataType) {
        for (int i = 0; i < relDataType.getFieldNames().size(); i++) {
            if (fieldName.equals(relDataType.getFieldNames().get(i))) {
                return i;
            }
        }
        return -1;
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
        List<String> result = new ArrayList<>();
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
        return result;
    }

    /**
     * check two bit set list is same.
     *
     * @param list1 list1
     * @param list2 list2
     * @return boolean
     */
    private static boolean differImmutableList(
            ImmutableList<ImmutableBitSet> list1, ImmutableList<ImmutableBitSet> list2) {
        if (list1.size() != list2.size()) {
            return true;
        }
        for (int i = 0; i < list1.size(); i++) {
            ImmutableBitSet bs1 = list1.get(i);
            ImmutableBitSet bs2 = list2.get(i);
            if (!bs1.equals(bs2)) {
                return true;
            }
        }
        return false;
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
        final List<Integer> result = new ArrayList<>();
        for (String field : fields) {
            int index = findIdByName(field, to.getRowType());
            if (index == -1) {
                return null;
            }
            result.add(index);
        }
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
    public static class Config extends MergeConfig<Aggregate, Aggregate> {

        public static final Config DEFAULT = new Config(Aggregate.class, Aggregate.class);

        public Config(Class<Aggregate> fromRelNodeType, Class<Aggregate> toRelNodeType) {
            super(fromRelNodeType, toRelNodeType);
        }

        @Override
        public AggregateMergeRule toMergeRule() {
            return new AggregateMergeRule(this);
        }
    }
}
