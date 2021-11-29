package io.agora.cruise.core.test;

import io.agora.cruise.core.Node;
import io.agora.cruise.core.ResultNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static io.agora.cruise.core.NodeUtils.findSubNode;

/** NodeTest. */
public class NodeTest {

    @Test
    public void testNode() {
        Node<String> root = new NodeString("1a");
        Node<String> b2 = new NodeString(root, "2b");
        new NodeString(root, "2c");
        new NodeString(root, "2d");
        new NodeString(b2, "3e");
        new NodeString(b2, "3f");

        Node<String> root2 = new NodeString("1a");
        new NodeString(root2, "2c");
        new NodeString(root2, "2d");

        List<ResultNode<String>> matchedResult = findSubNode(root, root2);
        Assert.assertEquals(matchedResult.size(), 1);
        ResultNode<String> resultNode = matchedResult.get(0);
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
    }
}
