package io.agora.cruise.core.merge;

import com.google.common.collect.ImmutableList;
import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** AggregationMergeable. */
public class AggregationMergeable {

    /**
     * merge from aggregate and ot aggregate with children node.
     *
     * @param fromAggregate from aggregate
     * @param toAggregate to aggregate
     * @param childrenResultNode children node
     * @return new aggregate
     */
    public static RelNode merge(
            Aggregate fromAggregate,
            Aggregate toAggregate,
            ResultNodeList<RelNode> childrenResultNode) {

        // aggregate only has one input
        if (childrenResultNode.size() != 1) {
            return null;
        }

        // create new group set
        RelNode newInput = childrenResultNode.get(0).getPayload();
        ImmutableBitSet newGroupSet = createNewGroupSet(fromAggregate, toAggregate, newInput);
        if (newGroupSet == null) {
            return null;
        }

        List<AggregateCall> newAggCalls =
                createNewAggregateCalls(fromAggregate, toAggregate, newInput);
        RelTraitSet newRelTraitSet = fromAggregate.getTraitSet().merge(toAggregate.getTraitSet());
        return RelNodeMergeable.copy(
                fromAggregate.copy(newRelTraitSet, newInput, newGroupSet, null, newAggCalls),
                childrenResultNode);
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
        List<String> fields = createNameByBitSetType(bitSet, from.getRowType());
        if (fields == null) {
            return null;
        }
        List<Integer> result = new ArrayList<>();
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
        if (bitSet == null) {
            return null;
        }
        List<String> fields = createNameByBitSetType(bitSet, from.getRowType());
        if (fields == null) {
            return null;
        }
        List<Integer> result = new ArrayList<>();
        for (String field : fields) {
            int index = findIdByName(field, to.getRowType());
            if (index == -1) {
                return null;
            }
            result.add(index);
        }
        return result;
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
        List<Integer> newArgIds =
                reMappingBitSet(call.getArgList(), parentAggregate.getInput(), newInput);
        if (newArgIds == null) {
            return null;
        }
        int newFilterArg =
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

        List<AggregateCall> newAggCalls = new ArrayList<>();
        for (int i = 0; i < fromAggregate.getAggCallList().size(); i++) {
            AggregateCall call = fromAggregate.getAggCallList().get(i);
            AggregateCall newCall = newAggregateCall(call, fromAggregate, newInput);
            if (newCall == null) {
                return null;
            }
            newAggCalls.add(newCall);
        }

        for (int i = 0; i < toAggregate.getAggCallList().size(); i++) {
            AggregateCall call = toAggregate.getAggCallList().get(i);
            AggregateCall newCall = newAggregateCall(call, toAggregate, newInput);
            if (newCall == null) {
                return null;
            }
            if (!newAggCalls.contains(newCall)) {
                newAggCalls.add(newCall);
            }
        }
        return newAggCalls;
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
        List<Integer> newGroupSet = new ArrayList<>();
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

        if (!sameImmutableList(
                        fromAggregate.getGroupSets(), ImmutableList.of(fromAggregate.getGroupSet()))
                || !sameImmutableList(
                        toAggregate.getGroupSets(), ImmutableList.of(toAggregate.getGroupSet()))) {
            return null;
        }
        List<String> fromGroupFields = groupSetNames(fromAggregate);
        List<String> toGroupFields = groupSetNames(toAggregate);
        Map<String, AggregateCall> fromNameCallMapping = new HashMap<>();
        for (int i = 0; i < fromAggregate.getRowType().getFieldNames().size(); i++) {
            String aggregationName = fromAggregate.getRowType().getFieldNames().get(i);
            if (fromGroupFields.contains(aggregationName)) {
                continue;
            }
            AggregateCall call = fromAggregate.getAggCallList().get(i - fromGroupFields.size());
            AggregateCall newCall = newAggregateCall(call, fromAggregate, input);
            fromNameCallMapping.put(aggregationName, newCall);
        }
        Map<String, AggregateCall> toNameCallMapping = new HashMap<>();
        for (int i = 0; i < toAggregate.getRowType().getFieldNames().size(); i++) {
            String aggregationName = toAggregate.getRowType().getFieldNames().get(i);
            if (toGroupFields.contains(aggregationName)) {
                continue;
            }

            AggregateCall call = toAggregate.getAggCallList().get(i - toGroupFields.size());
            AggregateCall newCall = newAggregateCall(call, toAggregate, input);
            toNameCallMapping.put(aggregationName, newCall);
        }
        for (Map.Entry<String, AggregateCall> entry : fromNameCallMapping.entrySet()) {
            AggregateCall toCall = toNameCallMapping.get(entry.getKey());
            if (toCall == null) {
                continue;
            }
            if (!entry.getValue().equals(toCall)) {
                return null;
            }
        }

        List<Integer> newGroupSet =
                sameIndexMapping(fromGroupFields, toGroupFields, input.getRowType());
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
     * @param bitSet bit set
     * @param relDataType RelDataType
     * @return list string
     */
    private static List<String> createNameByBitSetType(
            List<Integer> bitSet, RelDataType relDataType) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < bitSet.size(); i++) {
            if (relDataType.getFieldNames().size() <= bitSet.get(i)) {
                return null;
            }
            result.add(relDataType.getFieldNames().get(bitSet.get(i)));
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
    private static boolean sameImmutableList(
            ImmutableList<ImmutableBitSet> list1, ImmutableList<ImmutableBitSet> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); i++) {
            ImmutableBitSet bs1 = list1.get(i);
            ImmutableBitSet bs2 = list2.get(i);
            if (!bs1.equals(bs2)) {
                return false;
            }
        }
        return true;
    }
}
