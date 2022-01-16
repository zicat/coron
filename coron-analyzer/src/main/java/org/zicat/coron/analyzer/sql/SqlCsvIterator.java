/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.analyzer.sql;

import com.csvreader.CsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;

/** SqlCsv. */
public class SqlCsvIterator extends BasicSqlIterator {

    private static final Logger LOG = LoggerFactory.getLogger(SqlCsvIterator.class);

    protected CsvReader csvReader;
    protected final CsvParser parser;
    protected int i = 0;

    public SqlCsvIterator(Reader reader, CsvParser parser) {
        this.parser = parser;
        CsvReader tmpReader = null;
        try {
            tmpReader = new CsvReader(reader);
            tmpReader.readHeaders();
        } catch (Exception e) {
            LOG.error("parser csv error", e);
        }
        this.csvReader = tmpReader;
    }

    @Override
    public boolean hasNext() {
        try {
            return hasNext(csvReader != null && csvReader.readRecord());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String next() {
        i = i + 1;
        return parser.sql(csvReader);
    }

    @Override
    public void close() {
        if (csvReader != null) {
            csvReader.close();
            csvReader = null;
        }
    }

    @Override
    public int currentOffset() {
        return i;
    }

    /** CsvParser. */
    public interface CsvParser {

        CsvParser FIRST_COLUMN =
                csvReader -> {
                    try {
                        return csvReader.get(0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                };

        /**
         * parser sql.
         *
         * @param csvReader csvReader
         * @return sql
         */
        String sql(CsvReader csvReader);
    }
}
