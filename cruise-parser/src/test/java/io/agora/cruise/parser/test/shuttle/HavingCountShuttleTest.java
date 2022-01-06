package io.agora.cruise.parser.test.shuttle;

import io.agora.cruise.parser.SqlNodeUtils;
import io.agora.cruise.parser.sql.shuttle.HavingCountShuttle;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

/** HavingCountShuttleTest. */
public class HavingCountShuttleTest {

    @Test
    public void test() throws SqlParseException {

        String querySql = "select * from table1 having count(1) > 0";
        String querySql2 = "select * from table1 having count(*) > 0";

        String expectSql = "SELECT *\nFROM `table1`";

        Assert.assertEquals(
                expectSql,
                SqlNodeUtils.toSqlNode(
                                querySql,
                                SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG,
                                new HavingCountShuttle())
                        .toString());
        Assert.assertEquals(
                expectSql,
                SqlNodeUtils.toSqlNode(
                                querySql2,
                                SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG,
                                new HavingCountShuttle())
                        .toString());

        String querySql3 = "select * from table1 having count(*) = 0";
        String expectSql3 = "SELECT *\nFROM `table1`\nHAVING COUNT(*) = 0";
        Assert.assertEquals(
                expectSql3,
                SqlNodeUtils.toSqlNode(
                                querySql3,
                                SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG,
                                new HavingCountShuttle())
                        .toString());
    }
}
