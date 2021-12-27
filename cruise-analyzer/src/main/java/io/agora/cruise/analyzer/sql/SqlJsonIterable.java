package io.agora.cruise.analyzer.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** SqlJsonIterable. */
public class SqlJsonIterable extends SqlIterable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    protected SqlJsonIterator.JsonParser parser;
    protected JsonNode jsonNode;

    public SqlJsonIterable(String fileName, SqlJsonIterator.JsonParser parser) {
        this(fileName, StandardCharsets.UTF_8, parser);
        Reader reader = null;
        try {
            reader = createReader();
            this.jsonNode = MAPPER.readTree(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    public SqlJsonIterable(String fileName, Charset charset, SqlJsonIterator.JsonParser parser) {
        super(fileName, charset);
        this.parser = parser;
        Reader reader = null;
        try {
            reader = createReader();
            this.jsonNode = MAPPER.readTree(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    @Override
    public SqlIterator sqlIterator() {
        return new SqlJsonIterator(jsonNode, parser);
    }
}
