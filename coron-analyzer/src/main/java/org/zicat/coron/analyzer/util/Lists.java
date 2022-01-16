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

import java.util.ArrayList;
import java.util.List;

/** Lists. */
public class Lists {

    /**
     * merge list from and to.
     *
     * @param from from
     * @param to to
     * @param <T> type
     * @return new list
     */
    public static <T> List<T> merge(List<T> from, List<T> to) {
        if (from == null && to == null) {
            return new ArrayList<>();
        }
        if (from == null) {
            return new ArrayList<>(to);
        }
        List<T> combo = new ArrayList<>(from);
        if (to != null) {
            combo.addAll(to);
        }
        return combo;
    }
}
