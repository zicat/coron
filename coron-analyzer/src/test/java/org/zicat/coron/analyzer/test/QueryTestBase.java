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

package org.zicat.coron.analyzer.test;

import org.zicat.coron.analyzer.SqlAnalyzer;
import org.zicat.coron.analyzer.sql.SqlFilter;
import org.zicat.coron.analyzer.sql.dialect.PrestoDialect;
import org.zicat.coron.parser.CalciteContext;

/** QueryTestBase. */
public class QueryTestBase extends CalciteContext {

    protected SqlFilter sqlFilter =
            sql ->
                    sql.startsWith("explain")
                            || sql.contains("system.runtime")
                            || sql.startsWith("SHOW ")
                            || sql.startsWith("Show ")
                            || sql.startsWith("show ")
                            || sql.contains("levels_usage_dod_di_11");
    protected SqlAnalyzer.ExceptionHandler exceptionHandler =
            (sql, e) -> {
                String eString = e.toString();
                if (!eString.contains("Object 'media' not found")
                        && !eString.contains("Object 'queries' not found")
                        && !eString.contains("Object 'information_schema' not found")
                        && !eString.contains("Object 'vendor_vid_sku_final_di_1' not found")
                        && !eString.contains("not found in any table")
                        && !eString.contains("Column 'date' is ambiguous")) {
                    e.printStackTrace();
                }
            };

    public QueryTestBase() {
        super("test_db");
        addTestTableMeta();
    }

    protected void addTestTableMeta() {
        addTables(
                "CREATE TABLE test_db.test_table(d1 varchar, d2 varchar, m1 bigint, m2 bigint, date int)");
    }

    /**
     * create SubSql Tool.
     *
     * @return SubSqlTool
     */
    public SqlAnalyzer createSqlAnalyzer() {
        return new SqlAnalyzer(sqlFilter, exceptionHandler, this, PrestoDialect.DEFAULT);
    }
}
