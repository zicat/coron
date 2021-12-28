package io.agora.cruise.analyzer.sql;

import java.nio.charset.Charset;

/** SqlTextIterable. */
public class SqlTextIterable extends SqlReaderIterable {

    public SqlTextIterable(String fileName, Charset charset) {
        super(fileName, charset);
    }

    @Override
    public SqlIterator sqlIterator() {
        return new SqlTextIterator(createReader());
    }
}
