package io.agora.cruise.analyzer.metrics;

/** Metrics. */
public class Metrics {

    private long totalSpend;

    private long totalSql2NodeSpend;

    private long totalSubSqlSpend;

    private long totalNode2SqlSpend;

    public Metrics() {}

    public Metrics addTotalSpend(long spend) {
        this.totalSpend += spend;
        return this;
    }

    public Metrics addTotalSql2NodeSpend(long spend) {
        this.totalSql2NodeSpend += spend;
        return this;
    }

    public Metrics addTotalSubSqlSpend(long spend) {
        this.totalSubSqlSpend += spend;
        return this;
    }

    public Metrics addTotalNode2SqlSpend(long spend) {
        this.totalNode2SqlSpend += spend;
        return this;
    }

    @Override
    public String toString() {

        return "metrics: totalSpend="
                + totalSpend
                + ", totalSql2NodeSpend="
                + totalSql2NodeSpend
                + ", totalSubSqlSpend="
                + totalSubSqlSpend
                + ", totalNode2SqlSpend="
                + totalNode2SqlSpend;
    }
}
