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

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/** SqlJson. */
public class SqlJsonIterator extends BasicSqlIterator {

    private static final Logger LOG = LoggerFactory.getLogger(SqlJsonIterator.class);

    protected final Iterator<JsonNode> iterator;
    protected final JsonParser parser;
    protected int i = 0;

    public SqlJsonIterator(JsonNode jsonNode, JsonParser parser) {
        this.parser = parser;
        Iterator<JsonNode> it = null;
        try {
            it = jsonNode.iterator();
        } catch (Throwable e) {
            LOG.error("parser json error", e);
        }
        iterator = it;
    }

    @Override
    public boolean hasNext() {
        return hasNext(iterator != null && iterator.hasNext());
    }

    @Override
    public String next() {
        i = i + 1;
        return parser.sql(iterator.next());
    }

    @Override
    public int currentOffset() {
        return i;
    }

    @Override
    public void close() {}

    /** JsonParser. */
    public interface JsonParser {

        /**
         * parser json node to sql.
         *
         * @param jsonNode jsonNode
         * @return sql
         */
        String sql(JsonNode jsonNode);
    }
}
