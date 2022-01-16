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

package org.zicat.coron.core.merge.rule;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * AggregateFilterFinder.
 *
 * <p>Find the first filter in input RelNode.
 */
public class TopFilterFinder extends RelShuttleImpl {

    private final List<Filter> filters = new ArrayList<>();

    /**
     * Find the first filter from input rel node.
     *
     * @param relNode rel node
     * @return filter
     */
    public static List<Filter> find(RelNode relNode) {
        TopFilterFinder finder = new TopFilterFinder();
        relNode.accept(finder);
        return finder.filters;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        filters.add(filter);
        return super.visit(filter);
    }
}
