package io.agora.cruise.presto.sql;

/** SqlFilter. */
public interface SqlFilter {

    /**
     * filter sql.
     *
     * @param sql sql
     * @return boolean
     */
    boolean filter(String sql);
}
