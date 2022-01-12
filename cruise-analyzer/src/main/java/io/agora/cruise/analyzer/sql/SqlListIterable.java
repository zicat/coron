package io.agora.cruise.analyzer.sql;

import java.util.List;

/** SqlListIterable. */
public class SqlListIterable extends SqlIterable {

    private List<String> sql;

    public SqlListIterable(List<String> sql) {
        this.sql = sql;
    }

    @Override
    public SqlIterator sqlIterator() {
        return new SqlListIterator(sql);
    }

    @Override
    public int size() {
        return sql.size();
    }
}
