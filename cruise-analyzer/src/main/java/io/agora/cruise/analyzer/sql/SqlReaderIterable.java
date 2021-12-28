package io.agora.cruise.analyzer.sql;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** SqlReaderIterable. */
public abstract class SqlReaderIterable extends SqlIterable {

    protected final String fileName;
    protected final Charset charset;

    public SqlReaderIterable(String fileName, Charset charset) {
        this.fileName = fileName;
        this.charset = charset;
    }

    public SqlReaderIterable(String fileName) {
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
}
