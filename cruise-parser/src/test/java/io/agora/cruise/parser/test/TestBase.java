package io.agora.cruise.parser.test;

import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.sql.parser.SqlParseException;

/** TestBase. */
public class TestBase extends CalciteContext {

    protected String ddl1 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table (c INT64, a STRING, b VARCHAR)";
    protected String ddl2 =
            "CREATE TABLE IF NOT EXISTS test_db2.test_table2 (c INT64, a STRING, b VARCHAR)";

    public TestBase() throws SqlParseException {
        super();
        addTables(ddl1, ddl2);
    }
}
