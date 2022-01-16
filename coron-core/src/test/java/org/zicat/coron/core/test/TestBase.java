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

package org.zicat.coron.core.test;

import org.zicat.coron.parser.CalciteContext;

/** TestBase. */
public class TestBase extends CalciteContext {

    protected String ddl1 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table (c INT64, d INT64, a INT32, b INT32)";

    protected String ddl2 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table2 (e INT64, f BIGINT, g INT32, h INT32)";

    public TestBase() {
        super();
        addTables(ddl1, ddl2);
    }
}