/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.analyzer.util;

import org.apache.calcite.rel.RelNode;

import java.util.Collection;

/** RelNodeUtils. */
public class RelNodeUtils {

    /**
     * check relNode whether contains at least on type.
     *
     * @param relNode relNode
     * @param types type
     * @return boolean contains.
     */
    public static boolean containsKind(RelNode relNode, Collection<Class<?>> types) {
        for (Class<?> type : types) {
            if (type.isAssignableFrom(relNode.getClass())) {
                return true;
            }
        }
        boolean oneContains = false;
        for (int i = 0; i < relNode.getInputs().size(); i++) {
            if (containsKind(relNode.getInput(i), types)) {
                oneContains = true;
                break;
            }
        }
        return oneContains;
    }
}
