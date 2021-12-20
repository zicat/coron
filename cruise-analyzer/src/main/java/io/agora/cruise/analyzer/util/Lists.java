package io.agora.cruise.analyzer.util;

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
