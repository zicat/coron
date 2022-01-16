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

package org.zicat.coron.core.merge;

/** TwoMergeType. */
public class Operand {

    private static final Class<?> ENY_NODE_TYPE = Object.class;

    protected final Class<?> fromRelNodeType;
    protected final Class<?> toRelNodeType;
    protected Operand parent;

    private Operand(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        this.fromRelNodeType = fromRelNodeType;
        this.toRelNodeType = toRelNodeType;
    }

    public Operand operand(Operand parent) {
        this.parent = parent;
        return this;
    }

    /**
     * create Operand by fromRelNodeType and toRelNodeType.
     *
     * @param fromRelNodeType fromRelNodeType
     * @param toRelNodeType toRelNodeType
     * @return Operand
     */
    public static Operand of(Class<?> fromRelNodeType, Class<?> toRelNodeType) {
        return new Operand(fromRelNodeType, toRelNodeType);
    }

    /**
     * create operand by fromRelNodeType, toRelNodeType = ENY_NODE_TYPE.
     *
     * @param fromRelNodeType fromRelNodeType
     * @return Operand
     */
    public static Operand ofFrom(Class<?> fromRelNodeType) {
        return new Operand(fromRelNodeType, ENY_NODE_TYPE);
    }

    /**
     * create operand by toRelNodeType, fromRelNodeType = ENY_NODE_TYPE.
     *
     * @param toRelNodeType fromRelNodeType
     * @return Operand
     */
    public static Operand ofTo(Class<?> toRelNodeType) {
        return new Operand(ENY_NODE_TYPE, toRelNodeType);
    }

    /**
     * from rel node type is any.
     *
     * @return true if any from node type
     */
    public final boolean isAnyFromNodeType() {
        return fromRelNodeType == ENY_NODE_TYPE;
    }

    /**
     * to rel node type is any.
     *
     * @return true if any to node type
     */
    public final boolean isAnyToNodeType() {
        return toRelNodeType == ENY_NODE_TYPE;
    }

    public final Class<?> fromRelNodeType() {
        return fromRelNodeType;
    }

    public final Operand parent() {
        return parent;
    }

    public final Class<?> toRelNodeType() {
        return toRelNodeType;
    }
}
