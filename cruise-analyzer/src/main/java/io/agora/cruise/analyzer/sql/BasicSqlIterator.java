package io.agora.cruise.analyzer.sql;

import org.apache.commons.io.IOUtils;

/** SqlReader. */
public abstract class BasicSqlIterator implements SqlIterator {

    protected volatile boolean closed = false;

    /** check close. */
    protected boolean hasNext(boolean hasNext) {
        if (!hasNext && !closed) {
            closed = true;
            IOUtils.closeQuietly(this);
        }
        return hasNext;
    }
}
