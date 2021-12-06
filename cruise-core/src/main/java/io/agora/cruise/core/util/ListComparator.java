package io.agora.cruise.core.util;

import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

/**
 * ListComparator.
 *
 * @param <T>
 */
public class ListComparator<T extends Comparable<T>> implements Comparator<List<T>> {

    private final boolean asc;

    public ListComparator(boolean asc) {
        this.asc = asc;
    }

    public ListComparator() {
        this(true);
    }

    @Override
    public int compare(List<T> o1, List<T> o2) {

        if (o1 == null) {
            return 1;
        }
        if (o2 == null) {
            return -1;
        }
        final ListIterator<T> e1 = o1.listIterator();
        final ListIterator<T> e2 = o2.listIterator();
        while (e1.hasNext() && e2.hasNext()) {
            final T n1 = e1.next();
            final T n2 = e2.next();
            if (n1 == null) {
                return 1;
            }
            if (n2 == null) {
                return -1;
            }
            int compare = n1.compareTo(n2);
            if (compare > 0) {
                return asc ? 1 : -1;
            } else if (compare < 0) {
                return asc ? -1 : 1;
            }
        }
        if (e1.hasNext()) {
            return -1;
        }
        if (e2.hasNext()) {
            return 1;
        }
        return 0;
    }
}
