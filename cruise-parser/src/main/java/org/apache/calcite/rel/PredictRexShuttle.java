package org.apache.calcite.rel;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;

import java.util.ArrayList;
import java.util.List;

/** PredictRexShuttle. */
public class PredictRexShuttle extends RexShuttle {

    private final List<Integer> predicts = new ArrayList<>();

    public List<Integer> getPredicts() {
        return predicts;
    }

    @Override
    public RexNode visitTableInputRef(RexTableInputRef ref) {
        predicts.add(ref.getIndex());
        predicts.sort(Integer::compareTo);
        return super.visitTableInputRef(ref);
    }

    /**
     * get all predicts.
     *
     * @param node node
     * @return list predicts
     */
    public static List<Integer> predicts(RexNode node) {
        PredictRexShuttle predictRexShuttle = new PredictRexShuttle();
        node.accept(predictRexShuttle);
        return predictRexShuttle.getPredicts();
    }
}
