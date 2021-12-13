package io.agora.cruise.presto.sql;

import java.nio.charset.Charset;

/** SqlJsonIterable. */
public class SqlJsonIterable extends SqlIterable {

    protected SqlJsonIterator.JsonParser parser;

    public SqlJsonIterable(String fileName, SqlJsonIterator.JsonParser parser) {
        super(fileName);
        this.parser = parser;
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
