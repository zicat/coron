package io.agora.cruise.analyzer.sql;

import java.util.Iterator;

/** SqlIterable. */
public abstract class SqlIterable implements Iterable<String> {

    @Override
    public final Iterator<String> iterator() {
        return sqlIterator();
    }

    /**
     * create SqlIterator.
     *
     * @return SqlIterator
     */
    public abstract SqlIterator sqlIterator();

    /**
     * calculate size.
     *
     * @return count
     */
    public int size() {

        int size = 0;
        SqlIterator it = sqlIterator();
        while (it.hasNext()) {
            it.next();
            size++;
        }
        return size;
    }
}
