package io.agora.cruise.analyzer.sql;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** SqlJsonIterable. */
public class SqlJsonIterable extends SqlIterable {

    protected SqlJsonIterator.JsonParser parser;

    public SqlJsonIterable(String fileName, SqlJsonIterator.JsonParser parser) {
        this(fileName, StandardCharsets.UTF_8, parser);
    }

    public SqlJsonIterable(String fileName, Charset charset, SqlJsonIterator.JsonParser parser) {
        super(fileName, charset);
        this.parser = parser;
    }

    @Override
    public SqlIterator sqlIterator() {
        return new SqlJsonIterator(createReader(), parser);
    }
}
