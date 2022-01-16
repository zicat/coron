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

import org.apache.calcite.rel.core.TableScan;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** TableRelShuttleImpl. */
public class TableRelShuttleImpl extends RelShuttleImpl {

    private Set<String> tables = new HashSet<>();

    @Override
    public RelNode visit(TableScan tableScan) {
        tables.add(String.join(".", tableScan.getTable().getQualifiedName()));
        return tableScan;
    }

    public Set<String> getTables() {
        return tables;
    }

    /**
     * find tables.
     *
     * @param relRoot rel root
     * @return table name
     */
    public static Set<String> tables(RelRoot relRoot) {
        return tables(relRoot.rel);
    }

    /**
     * find tables.
     *
     * @param relNode rel root
     * @return table name
     */
    public static Set<String> tables(RelNode relNode) {
        TableRelShuttleImpl tableRelShuttle = new TableRelShuttleImpl();
        relNode.accept(tableRelShuttle);
        return tableRelShuttle.getTables();
    }

    /**
     * find table string.
     *
     * @param relRoot rel root
     * @return path
     */
    public static String tableString(RelRoot relRoot) {
        return tableString(relRoot.rel);
    }

    /**
     * find table string.
     *
     * @param relNode rel root
     * @return path
     */
    public static String tableString(RelNode relNode) {
        Set<String> tables = tables(relNode);
        return tables == null ? null : formatPath(tables);
    }

    /**
     * format to string.
     *
     * @param sourcePaths source paths
     * @return string path
     */
    public static String formatPath(Set<String> sourcePaths) {
        if (sourcePaths == null) {
            return null;
        }
        return sourcePaths.stream().sorted(Comparator.reverseOrder()).collect(Collectors.joining());
    }
}
