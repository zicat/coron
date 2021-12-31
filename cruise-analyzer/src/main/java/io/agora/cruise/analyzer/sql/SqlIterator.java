package io.agora.cruise.analyzer.sql;

import java.io.Closeable;
import java.util.Iterator;

/** SqlReader. */
public interface SqlIterator extends Iterator<String>, Closeable {

    /**
     * get current offset.
     *
     * @return offset
     */
    int currentOffset();
}
