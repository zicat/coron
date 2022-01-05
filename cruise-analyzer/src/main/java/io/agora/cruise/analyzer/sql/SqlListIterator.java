package io.agora.cruise.analyzer.sql;

import java.util.Iterator;
import java.util.List;

/** SqlListIterator. */
public class SqlListIterator implements SqlIterator {

    private Iterator<String> it;
    private int offset = 0;

    public SqlListIterator(List<String> sql) {
        this.it = sql.iterator();
    }

    @Override
    public int currentOffset() {
        return offset;
    }

    @Override
    public void close() {}

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public String next() {
        offset++;
        return it.next();
    }
}
