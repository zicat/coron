package io.agora.cruise.analyzer.sql;

import java.nio.charset.Charset;

/** SqlCsvIterable. */
public class SqlCsvIterable extends SqlIterable {

    protected final SqlCsvIterator.CsvParser parser;

    public SqlCsvIterable(String fileName, Charset charset, SqlCsvIterator.CsvParser parser) {
        super(fileName, charset);
        this.parser = parser;
    }

    public SqlCsvIterable(String fileName, SqlCsvIterator.CsvParser parser) {
        super(fileName);
        this.parser = parser;
    }

    @Override
    public SqlIterator sqlIterator() {
        return new SqlCsvIterator(createReader(), parser);
    }
}
