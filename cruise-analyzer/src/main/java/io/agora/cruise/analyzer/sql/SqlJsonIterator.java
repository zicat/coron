package io.agora.cruise.analyzer.sql;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/** SqlJson. */
public class SqlJsonIterator extends BasicSqlIterator {

    private static final Logger LOG = LoggerFactory.getLogger(SqlJsonIterator.class);

    protected final Iterator<JsonNode> iterator;
    protected final JsonParser parser;
    protected int i = 0;

    public SqlJsonIterator(JsonNode jsonNode, JsonParser parser) {
        this.parser = parser;
        Iterator<JsonNode> it = null;
        try {
            it = jsonNode.iterator();
        } catch (Throwable e) {
            LOG.error("parser json error", e);
        }
        iterator = it;
    }

    @Override
    public boolean hasNext() {
        return hasNext(iterator != null && iterator.hasNext());
    }

    @Override
    public String next() {
        i = i + 1;
        return parser.sql(iterator.next());
    }

    @Override
    public int currentOffset() {
        return i;
    }

    @Override
    public void close() {}

    /** JsonParser. */
    public interface JsonParser {

        /**
         * parser json node to sql.
         *
         * @param jsonNode jsonNode
         * @return sql
         */
        String sql(JsonNode jsonNode);
    }
}
