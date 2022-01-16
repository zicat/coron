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

package org.zicat.coron.core.test;

import org.zicat.coron.core.Node;
import org.zicat.coron.core.ResultNode;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.coron.core.NodeUtils;

/** NodeTest. */
public class NodeTest {

    @Test
    public void testBrotherChildMerge() {

        /*
         from***** l0 -> l1 -> l2a -> l3a
            ****** .........-> l2b -> l3b
            ****** .........-> l2c -> l3c
        */
        Node<String> l0 = new NodeString("l0");
        Node<String> l1 = new NodeString(l0, "l1");
        Node<String> l2a = new NodeString(l1, "l2a");
        Node<String> l2b = new NodeString(l1, "l2b");
        Node<String> l2c = new NodeString(l1, "l2c");
        new NodeString(l2a, "l3a");
        new NodeString(l2b, "l3b");
        new NodeString(l2c, "l3c");

        /*
         to ****** l1 -> l2a -> l3a
            ****** ...-> l2b -> l3b
            ****** ...-> l2c -> l3c
        */
        Node<String> pl1 = new NodeString("l1");
        Node<String> pl2a = new NodeString(pl1, "l2a");
        Node<String> pl2b = new NodeString(pl1, "l2b");
        Node<String> pl2c = new NodeString(pl1, "l2c");
        new NodeString(pl2a, "l3a");
        new NodeString(pl2b, "l3b");
        new NodeString(pl2c, "l3c");

        ResultNode<String> resultNode = NodeUtils.findFirstSubNode(l0, pl1);
        Assert.assertEquals("l1", resultNode.getPayload());
    }

    @Test
    public void testNode() {

        /*
         from***** 1a -> 2b -> 3e
             ***** .........-> 3f
             ***** ...-> 2c
             ***** ...-> 2d
        */
        Node<String> root = new NodeString("1a");
        Node<String> b2 = new NodeString(root, "2b");
        new NodeString(root, "2c");
        new NodeString(root, "2d");
        new NodeString(b2, "3e");
        new NodeString(b2, "3f");

        /*
         to****** 1a -> 2c
           ****** ...-> 2d
        */
        Node<String> root2 = new NodeString("1a");
        new NodeString(root2, "2c");
        new NodeString(root2, "2d");

        ResultNode<String> resultNode = NodeUtils.findFirstSubNode(root, root2);
        Assert.assertFalse(resultNode.isEmpty());
        Assert.assertEquals(resultNode.getPayload(), "1a");
        Assert.assertEquals(resultNode.getChildren().size(), 2);
        Assert.assertEquals(resultNode.getChildren().get(0).getPayload(), "2c");
        Assert.assertEquals(resultNode.getChildren().get(1).getPayload(), "2d");
    }

    /** NodeString. */
    public static class NodeString extends Node<String> {

        protected NodeString(String payload) {
            super(payload);
        }

        protected NodeString(Node<String> parent, String payload) {
            super(parent, payload);
        }

        @Override
        public String toString() {
            return payload;
        }
    }
}
