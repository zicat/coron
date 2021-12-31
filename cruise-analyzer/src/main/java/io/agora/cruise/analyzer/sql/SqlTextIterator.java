package io.agora.cruise.analyzer.sql;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/** SqlTextIterator. */
public class SqlTextIterator extends BasicSqlIterator {

    private static final Logger LOG = LoggerFactory.getLogger(SqlJsonIterator.class);

    private int i = 0;
    private BufferedReader br;
    private String line;

    public SqlTextIterator(Reader reader) {
        this.br = new BufferedReader(reader);
        try {
            line = br.readLine();
        } catch (IOException e) {
            LOG.error("read text error", e);
        }
    }

    @Override
    public int currentOffset() {
        return i;
    }

    @Override
    public void close() {
        if (br != null) {
            IOUtils.closeQuietly(br);
            br = null;
        }
    }

    @Override
    public boolean hasNext() {
        return hasNext(line != null);
    }

    @Override
    public String next() {
        String result = line;
        try {
            i = i + 1;
            line = br.readLine();
        } catch (IOException e) {
            LOG.error("read text error", e);
            IOUtils.closeQuietly(this);
        }
        return result;
    }
}
