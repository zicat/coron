package io.agora.cruise.analyzer.util;

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
