package io.agora.cruise.presto.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.Iterator;

/** SqlJson. */
public class SqlJsonIterator extends BasicSqlIterator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(SqlJsonIterator.class);

    protected final Reader reader;
    protected final Iterator<JsonNode> iterator;
    protected final JsonParser parser;
    protected int i = 0;

    public SqlJsonIterator(Reader reader, JsonParser parser) {
        this.reader = reader;
        this.parser = parser;
        Iterator<JsonNode> it = null;
        try {
            it = MAPPER.readTree(reader).iterator();
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
    public void close() {
        IOUtils.closeQuietly(reader);
    }

    @Override
    public int currentOffset() {
        return i;
    }

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
