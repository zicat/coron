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
