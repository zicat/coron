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

package org.zicat.coron.analyzer.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Metrics. */
public class AnalyzerSpend {

    private final AtomicLong totalSql2NodeSpend = new AtomicLong();

    private final AtomicLong totalSqlAnalysisSpend = new AtomicLong();

    private final AtomicLong totalNode2SqlSpend = new AtomicLong();

    public AnalyzerSpend() {}

    public void addTotalSql2NodeSpend(long spend) {
        this.totalSql2NodeSpend.addAndGet(spend);
    }

    public void addTotalSubSqlSpend(long spend) {
        this.totalSqlAnalysisSpend.addAndGet(spend);
    }

    public void addTotalNode2SqlSpend(long spend) {
        this.totalNode2SqlSpend.addAndGet(spend);
    }

    @Override
    public String toString() {

        return "metrics: totalSql2NodeSpend="
                + totalSql2NodeSpend.get()
                + ", totalSubSqlSpend="
                + totalSqlAnalysisSpend.get()
                + ", totalNode2SqlSpend="
                + totalNode2SqlSpend.get();
    }
}
