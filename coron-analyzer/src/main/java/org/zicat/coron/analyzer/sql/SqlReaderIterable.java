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
