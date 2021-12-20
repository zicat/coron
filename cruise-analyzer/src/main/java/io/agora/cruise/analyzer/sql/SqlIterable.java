package io.agora.cruise.analyzer.sql;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/** SqlIterable. */
public abstract class SqlIterable implements Iterable<String> {

    protected final String fileName;
    protected final Charset charset;

    public SqlIterable(String fileName, Charset charset) {
        this.fileName = fileName;
        this.charset = charset;
    }

    public SqlIterable(String fileName) {
        this(fileName, StandardCharsets.UTF_8);
    }

    /**
     * create reader from classpath.
     *
     * @return reader
     */
    protected Reader createReader() {
        InputStream in =
                Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        if (in == null) {
            try {
                File file = new File(fileName).getCanonicalFile();
                in = new FileInputStream(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new InputStreamReader(in, charset);
    }

    @Override
    public final Iterator<String> iterator() {
        return sqlIterator();
    }

    /**
     * create SqlIterator.
     *
     * @return SqlIterator
     */
    public abstract SqlIterator sqlIterator();
}
