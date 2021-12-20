package io.agora.cruise.analyzer.sql;

import java.io.Closeable;
import java.util.Iterator;

/** SqlReader. */
public interface SqlIterator extends Iterator<String>, Closeable {

    /**
     * skip i sql.
     *
     * @param i i
     */
    void skip(int i);

    /**
     * get current offset.
     *
     * @return offset
     */
    int currentOffset();
}
