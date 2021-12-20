package io.agora.cruise.presto.sql;

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

    @Override
    public void skip(int i) {
        while (i != 0 && hasNext()) {
            next();
            i--;
        }
    }
}
