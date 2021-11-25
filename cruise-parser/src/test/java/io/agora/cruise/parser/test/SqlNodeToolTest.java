package io.agora.cruise.parser.test;

import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

/** SqlNodeToolTest. */
public class SqlNodeToolTest extends TestBase {

    String querySql = "SELECT a, b, sum(c) from t1 where a > '10' group by a, b";

    String expectQuerySql =
            "SELECT a, b, SUM(c)\n" + "FROM t1\n" + "WHERE a > '10'\n" + "GROUP BY a, b";

    public SqlNodeToolTest() throws SqlParseException {}

    @Test
    public void testQuerySql() throws SqlParseException {
        SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql);
        Assert.assertEquals(expectQuerySql, SqlNodeTool.toSql(sqlNode));
    }

    @Test
    public void testDDLSql() throws SqlParseException {
        SqlNode sqlNode = SqlNodeTool.toDDLSqlNode(ddl1);
        Assert.assertEquals(ddl1, SqlNodeTool.toSql(sqlNode));
    }
}
