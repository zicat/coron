package io.agora.cruise.core.merge;

import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        RelNode input = childrenResultNode.get(0).getPayload();

        Map<String, Integer> fromGroupFields = groupSetMapping(fromAggregate);
        Map<String, Integer> toGroupFields = groupSetMapping(toAggregate);
        List<Integer> newGroupSet =
                sameIndexMapping(fromGroupFields, toGroupFields, input.getRowType());
        if (newGroupSet == null) {
            return null;
        }

        if (fromAggregate.getAggCallList().size() != toAggregate.getAggCallList().size()) {
            return null;
        }
        List<AggregateCall> newAggCalls = new ArrayList<>();
        for (int i = 0; i < fromAggregate.getAggCallList().size(); i++) {
            AggregateCall fromCall = fromAggregate.getAggCallList().get(i);
            AggregateCall toCall = toAggregate.getAggCallList().get(i);

            // check distinct keys
            if (fromCall.distinctKeys == null && toCall.distinctKeys != null) {
                return null;
            }
            if (fromCall.distinctKeys != null && toCall.distinctKeys == null) {
                return null;
            }
            ImmutableBitSet newDistinctKey = null;
            if (fromCall.distinctKeys != null) {
                Map<String, Integer> fromDistinctFieldMapping =
                        getFieldById(fromCall.distinctKeys, fromAggregate.getInput().getRowType());
                Map<String, Integer> toDistinctFieldMapping =
                        getFieldById(toCall.distinctKeys, toAggregate.getInput().getRowType());
                List<Integer> newDistinctList =
                        sameIndexMapping(
                                fromDistinctFieldMapping,
                                toDistinctFieldMapping,
                                input.getRowType());
                if (newDistinctList == null) {
                    return null;
                }
                newDistinctKey = ImmutableBitSet.of(newDistinctList);
            }
            // check aggregation function(sum, max .eg)
            if (!fromCall.getAggregation().equals(toCall.getAggregation())) {
                return null;
            }

            // check aggregation function args
            List<String> fromCallArgNames =
                    fromCall.getArgList().stream()
                            .map(v -> fromAggregate.getInput().getRowType().getFieldNames().get(v))
                            .collect(Collectors.toList());
            List<String> toCallArgNames =
                    toCall.getArgList().stream()
                            .map(v -> toAggregate.getInput().getRowType().getFieldNames().get(v))
                            .collect(Collectors.toList());
            if (!fromCallArgNames.equals(toCallArgNames)) {
                return null;
            }
            List<Integer> newArgList = new ArrayList<>();
            for (String name : fromCallArgNames) {
                boolean found = false;
                for (int j = 0; j < input.getRowType().getFieldNames().size(); j++) {
                    if (input.getRowType().getFieldNames().get(j).equals(name)) {
                        newArgList.add(j);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
            }

            int newFilterAgg = -1;
            if ((fromCall.filterArg == -1 && toCall.filterArg != -1)
                    || toCall.filterArg == -1 && fromCall.filterArg != -1) {
                return null;
            }
            if (fromCall.filterArg != -1) {
                String fromFilterName =
                        fromAggregate
                                .getInput()
                                .getRowType()
                                .getFieldNames()
                                .get(fromCall.filterArg);
                String toFilterName =
                        toAggregate.getInput().getRowType().getFieldNames().get(toCall.filterArg);
                if (!fromFilterName.equals(toFilterName)) {
                    return null;
                }
                int offset = findIdByName(fromFilterName, input.getRowType());
                if (offset == -1) {
                    return null;
                }
                newFilterAgg = offset;
            }

            newAggCalls.add(
                    AggregateCall.create(
                            fromCall.getAggregation(),
                            fromCall.isDistinct(),
                            fromCall.isApproximate(),
                            fromCall.ignoreNulls(),
                            newArgList,
                            newFilterAgg,
                            newDistinctKey,
                            fromCall.collation,
                            fromCall.type,
                            fromCall.name));
        }

        fromAggregate.copy(
                fromAggregate.getTraitSet(),
                input,
                ImmutableBitSet.of(newGroupSet),
                null,
                newAggCalls);
        return RelNodeMergeable.copy(fromAggregate, childrenResultNode);
    }

    private static List<Integer> sameIndexMapping(
            Map<String, Integer> fromFieldIndexMap,
            Map<String, Integer> toFieldIndexMap,
            RelDataType relDataType) {

        if (fromFieldIndexMap.size() != toFieldIndexMap.size()) {
            return null;
        }
        List<Integer> newGroupSet = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : fromFieldIndexMap.entrySet()) {
            if (!toFieldIndexMap.containsKey(entry.getKey())) {
                return null;
            }
            String fieldName = entry.getKey();
            int offset = findIdByName(fieldName, relDataType);
            if (offset == -1) {
                return null;
            }
            newGroupSet.add(offset);
        }
        return newGroupSet;
    }

    private static int findIdByName(String fieldName, RelDataType relDataType) {
        for (int i = 0; i < relDataType.getFieldNames().size(); i++) {
            if (fieldName.equals(relDataType.getFieldNames().get(i))) {
                return i;
            }
        }
        return -1;
    }

    private static Map<String, Integer> groupSetMapping(Aggregate aggregate) {
        return getFieldById(aggregate.getGroupSet(), aggregate.getInput().getRowType());
    }

    private static Map<String, Integer> getFieldById(
            ImmutableBitSet bitSet, RelDataType relDataType) {
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < bitSet.size(); i++) {
            if (bitSet.get(i)) {
                String field = relDataType.getFieldNames().get(i);
                result.put(field, i);
            }
        }
        return result;
    }
}
