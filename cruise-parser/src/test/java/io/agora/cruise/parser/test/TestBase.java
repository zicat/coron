package io.agora.cruise.parser.test;

import io.agora.cruise.parser.CalciteContext;

/** TestBase. */
public class TestBase extends CalciteContext {

    protected String ddl1 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table (c INT64, a STRING, b VARCHAR, x VARCHAR)";
    protected String ddl2 =
            "CREATE TABLE IF NOT EXISTS test_db2.test_table2 (c INT64, a STRING, b VARCHAR, x VARCHAR)";

    public TestBase() {
        super();
        addTables(ddl1, ddl2);
    }
}
