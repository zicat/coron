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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** SqlJsonIterable. */
public class SqlJsonIterable extends SqlReaderIterable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    protected SqlJsonIterator.JsonParser parser;
    protected JsonNode jsonNode;

    public SqlJsonIterable(String fileName, SqlJsonIterator.JsonParser parser) {
        this(fileName, StandardCharsets.UTF_8, parser);
        Reader reader = null;
        try {
            reader = createReader();
            this.jsonNode = MAPPER.readTree(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    public SqlJsonIterable(String fileName, Charset charset, SqlJsonIterator.JsonParser parser) {
        super(fileName, charset);
        this.parser = parser;
        Reader reader = null;
        try {
            reader = createReader();
            this.jsonNode = MAPPER.readTree(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    @Override
    public SqlIterator sqlIterator() {
        return new SqlJsonIterator(jsonNode, parser);
    }

    @Override
    public int size() {
        return jsonNode.size();
    }
}
