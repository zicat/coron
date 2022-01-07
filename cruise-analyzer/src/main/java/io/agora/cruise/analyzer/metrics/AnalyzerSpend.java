package io.agora.cruise.analyzer.metrics;

import java.util.concurrent.atomic.AtomicLong;

/** Metrics. */
public class AnalyzerSpend {

    private final AtomicLong totalSql2NodeSpend = new AtomicLong();

    private final AtomicLong totalSubSqlSpend = new AtomicLong();

    private final AtomicLong totalNode2SqlSpend = new AtomicLong();

    public AnalyzerSpend() {}

    public AnalyzerSpend addTotalSql2NodeSpend(long spend) {
        this.totalSql2NodeSpend.addAndGet(spend);
        return this;
    }

    public AnalyzerSpend addTotalSubSqlSpend(long spend) {
        this.totalSubSqlSpend.addAndGet(spend);
        return this;
    }

    public AnalyzerSpend addTotalNode2SqlSpend(long spend) {
        this.totalNode2SqlSpend.addAndGet(spend);
        return this;
    }

    @Override
    public String toString() {

        return "metrics: totalSql2NodeSpend="
                + totalSql2NodeSpend.get()
                + ", totalSubSqlSpend="
                + totalSubSqlSpend.get()
                + ", totalNode2SqlSpend="
                + totalNode2SqlSpend.get();
    }
}
