package io.agora.cruise.core.test;

import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.sql.parser.SqlParseException;

/** TestBase. */
public class TestBase extends CalciteContext {

    protected String ddl1 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table (c INT64, d INT64, a INT32, b INT32)";

    protected String ddl2 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table2 (e INT64, f BIGINT, g INT32, h INT32)";

    public TestBase() throws SqlParseException {
        super();
        addTables(ddl1, ddl2);
    }
}
