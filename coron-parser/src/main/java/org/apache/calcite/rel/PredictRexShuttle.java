/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
