package io.agora.cruise.core;

import io.agora.cruise.core.merge.MergeConfig;
import io.agora.cruise.core.merge.RelNodeMergePlanner;
import io.agora.cruise.core.merge.rule.*;
import org.apache.calcite.rel.RelNode;

import java.util.*;

/** NodeUtils. */
public class NodeUtils {

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot, RelNodeMergePlanner mergePlanner) {
        return createNodeRelRoot(relRoot, mergePlanner, relNode -> relNode);
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @param simplify simplify
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(
            RelNode relRoot, RelNodeMergePlanner mergePlanner, NodeRel.Simplify simplify) {
        final Queue<NodeRel> queue = new LinkedList<>();
        final NodeRel nodeRoot = NodeRel.of(mergePlanner, simplify.apply(relRoot));
        queue.offer(nodeRoot);
        while (!queue.isEmpty()) {
            final Node<RelNode> node = queue.poll();
            final RelNode relNode = node.getPayload();
            for (int i = 0; i < relNode.getInputs().size(); i++) {
                queue.offer(NodeRel.of(mergePlanner, node, relNode.getInput(i)));
            }
        }
        return nodeRoot;
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot) {
        return createNodeRelRoot(relRoot, true);
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @param simplify simplify
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot, NodeRel.Simplify simplify) {
        return createNodeRelRoot(relRoot, true, simplify);
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @param canMaterialized materialized
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(RelNode relRoot, boolean canMaterialized) {
        final List<MergeConfig> mergeRuleConfigs = new ArrayList<>();
        if (canMaterialized) {
            mergeRuleConfigs.add(AggregateFilterMergeRule.Config.createFrom());
            mergeRuleConfigs.add(AggregateFilterMergeRule.Config.createTo());
        }
        mergeRuleConfigs.addAll(
                Arrays.asList(
                        TableScanMergeRule.Config.create().materialized(canMaterialized),
                        ProjectMergeRule.Config.create().materialized(canMaterialized),
                        FilterMergeRule.Config.create().materialized(canMaterialized),
                        AggregateMergeRule.Config.create().materialized(canMaterialized),
                        JoinMergeRule.Config.create().materialized(canMaterialized),
                        FilterProjectMergeRule.Config.create().materialized(canMaterialized),
                        ProjectFilterMergeRule.Config.create().materialized(canMaterialized)));
        return createNodeRelRoot(relRoot, new RelNodeMergePlanner(mergeRuleConfigs));
    }

    /**
     * create node rel root.
     *
     * @param relRoot rel root
     * @param canMaterialized materialized
     * @param simplify simplify
     * @return node rel
     */
    public static NodeRel createNodeRelRoot(
            RelNode relRoot, boolean canMaterialized, NodeRel.Simplify simplify) {
        final List<MergeConfig> mergeRuleConfigs =
                Arrays.asList(
                        TableScanMergeRule.Config.create().materialized(canMaterialized),
                        ProjectMergeRule.Config.create().materialized(canMaterialized),
                        FilterMergeRule.Config.create().materialized(canMaterialized),
                        AggregateMergeRule.Config.create().materialized(canMaterialized),
                        JoinMergeRule.Config.create().materialized(canMaterialized),
                        FilterProjectMergeRule.Config.create().materialized(canMaterialized),
                        ProjectFilterMergeRule.Config.create().materialized(canMaterialized));
        return createNodeRelRoot(relRoot, new RelNodeMergePlanner(mergeRuleConfigs), simplify);
    }

    /**
     * find all Sub Node list.
     *
     * @param rootFrom rootFrom
     * @param rootTo rootTo
     * @param <T> payload type
     * @return ResultNodeList
     */
    public static <T> ResultNodeList<T> findAllSubNode(Node<T> rootFrom, Node<T> rootTo) {

        final List<Node<T>> nodeFromLeaves = findAllFirstLeafNode(rootFrom);
        final List<Node<T>> nodeToLeaves = findAllFirstLeafNode(rootTo);
        final ResultNodeList<T> resultNodes = new ResultNodeList<>();

        for (Node<T> fromNode : nodeToLeaves) {
            for (Node<T> toNode : nodeFromLeaves) {
                final ResultNode<T> resultNode = merge(rootFrom, fromNode, toNode, false);
                resultNodes.add(resultNode);
            }
        }
        return resultNodes;
    }

    /**
     * find the same sub nodes from rootFrom and rootTo.
     *
     * @param rootFrom rootFrom
     * @param rootTo rootTo
     * @param <T> payload type
     * @return result node
     */
    public static <T> ResultNode<T> findFirstSubNode(Node<T> rootFrom, Node<T> rootTo) {

        final List<Node<T>> nodeFromLeaves = findAllFirstLeafNode(rootFrom);
        final List<Node<T>> nodeToLeaves = findAllFirstLeafNode(rootTo);

        for (Node<T> fromNode : nodeToLeaves) {
            for (Node<T> toNode : nodeFromLeaves) {
                final ResultNode<T> resultNode = merge(rootFrom, fromNode, toNode, false);
                if (!resultNode.isEmpty()) {
                    return resultNode;
                }
            }
        }
        return ResultNode.empty();
    }

    /**
     * merge start from fromLeaf and to toLeaf, end to fromRoot.
     *
     * @param fromRoot the root of from.
     * @param fromLeaf the leaf of the fromRoot
     * @param toLeaf the to leaf
     * @param allMatch is all match
     * @param <T> type
     * @return merge result if all match return from fromLeaf toLeaf to fromRoot mergeable, else
     *     return max partial merge result
     */
    private static <T> ResultNode<T> merge(
            Node<T> fromRoot, Node<T> fromLeaf, Node<T> toLeaf, boolean allMatch) {

        if (fromLeaf == null || toLeaf == null) {
            return ResultNode.empty();
        }

        ResultNodeList<T> resultOffset = new ResultNodeList<>(fromLeaf.merge(toLeaf));
        if (resultOffset.isEmpty()) {
            return ResultNode.empty();
        }

        Node<T> fromOffset = fromLeaf;
        Node<T> toOffset = toLeaf;
        ResultNode<T> maxResultNode = allMatch ? resultOffset.get(0) : ResultNode.empty();

        while (fromOffset != fromRoot && toOffset != null) {
            final int size = sameSize(fromOffset.rightBrotherSize(), toOffset.rightBrotherSize());
            for (int i = 0; i < size; i++) {
                ResultNode<T> brotherMergeResult =
                        merge(
                                fromOffset.rightBrother(i),
                                foundLeftLeaf(fromOffset.rightBrother(i)),
                                foundLeftLeaf(toOffset.rightBrother(i)),
                                true);
                if (brotherMergeResult.isEmpty()) {
                    break;
                }
                resultOffset.add(brotherMergeResult);
            }
            // size mean all brother equals, 1 mean me equal
            if (resultOffset.size() != size + 1) {
                break;
            }
            final ResultNode<T> parentResultNode = fromOffset.parentMerge(toOffset, resultOffset);
            if (parentResultNode.isEmpty()) {
                break;
            }
            resultOffset = new ResultNodeList<>(parentResultNode);
            fromOffset = lookAhead(fromOffset, parentResultNode.fromLookAhead, fromRoot);
            toOffset = lookAhead(toOffset, parentResultNode.toLookAhead, null);
            maxResultNode = parentResultNode;
        }
        return allMatch && (fromOffset != fromRoot) ? ResultNode.empty() : maxResultNode;
    }

    /**
     * found left leaf.
     *
     * @param node node
     * @param <T> type
     * @return node
     */
    public static <T> Node<T> foundLeftLeaf(Node<T> node) {
        if (node == null) {
            return null;
        }
        Node<T> result = node;
        while (!result.isLeaf()) {
            result = result.children.get(0);
        }
        return result;
    }

    /**
     * look ahead the input node.
     *
     * @param node node
     * @param ahead ahead
     * @param root root
     * @param <T> payload type
     * @return node
     */
    private static <T> Node<T> lookAhead(Node<T> node, int ahead, Node<T> root) {

        if (ahead < 0) {
            throw new RuntimeException("ahead must >= 0");
        }
        int offset = ahead;
        Node<T> result = node;
        while (offset > 0 && result != root) {
            result = result.parent;
            offset--;
        }
        return result;
    }

    /**
     * find all first leave nodes.
     *
     * <p>Example:
     *
     * <p>a -> b -> e
     *
     * <p>a -> b ->f
     *
     * <p>a -> c
     *
     * <p>a -> d
     *
     * <p>Got Node [e, c]
     *
     * @param node parent node
     * @param <T> payload
     * @return list
     */
    private static <T> List<Node<T>> findAllFirstLeafNode(Node<T> node) {

        final List<Node<T>> result = new ArrayList<>();
        if (node == null) {
            return result;
        }

        final Set<Node<T>> parent = new HashSet<>();
        final Stack<Node<T>> stack = new Stack<>();
        stack.push(node);
        while (!stack.isEmpty()) {
            final Node<T> popNode = stack.pop();
            if (!popNode.isLeaf()) {
                for (int i = popNode.children.size() - 1; i >= 0; i--) {
                    stack.push(popNode.children.get(i));
                }
            } else {
                if (!popNode.isRoot() && !parent.contains(popNode.parent)) {
                    result.add(popNode);
                    parent.add(popNode.parent);
                }
            }
        }
        return result;
    }

    /**
     * checkout two size is equals.
     *
     * @param size1 size1
     * @param size2 size2
     * @return if not equals return -1 else return size
     */
    private static int sameSize(int size1, int size2) {
        return size1 == size2 ? size1 : -1;
    }
}
