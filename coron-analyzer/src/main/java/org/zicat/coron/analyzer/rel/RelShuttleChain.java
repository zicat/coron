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

package org.zicat.coron.analyzer.rel;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RelShuttleChain. */
public class RelShuttleChain {

    private static final Logger LOG = LoggerFactory.getLogger(RelShuttleChain.class);

    private final RelShuttleImpl[] shuttles;

    public RelShuttleChain(RelShuttleImpl[] shuttles) {
        this.shuttles = shuttles;
    }

    /**
     * empty chain.
     *
     * @return RelShuttleChain
     */
    public static RelShuttleChain empty() {
        return new RelShuttleChain(null);
    }

    /**
     * create RelShuttleChain.
     *
     * @param shuttles shuttles
     * @return RelShuttleChain
     */
    public static RelShuttleChain of(RelShuttleImpl... shuttles) {
        return new RelShuttleChain(shuttles);
    }

    /**
     * translate RelNode struct by shuttles.
     *
     * @param relNode relNode
     * @return RelNode, null if accept error
     */
    public final RelNode accept(RelNode relNode) {

        if (relNode == null || shuttles == null) {
            return relNode;
        }
        RelNode result = relNode;
        for (RelShuttleImpl relShuttle : shuttles) {
            try {
                result = result.accept(relShuttle);
            } catch (RelShuttleChainException chainException) {
                LOG.warn(
                        "RelShuttle Convert Fail {}, {}",
                        relShuttle.getClass(),
                        chainException.getMessage());
                return null;
            } catch (Exception e) {
                LOG.warn("RelShuttle Convert Fail " + relShuttle.getClass(), e);
                return null;
            }
        }
        return result;
    }
}
