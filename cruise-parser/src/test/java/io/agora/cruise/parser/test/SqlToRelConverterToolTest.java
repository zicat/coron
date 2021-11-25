package io.agora.cruise.parser.test;

import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SqlToRelConverterToolTest. */
public class SqlToRelConverterToolTest extends TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(SqlToRelConverterToolTest.class);

    String querySql = "SELECT a, b, sum(c) from test_db.test_table where a > '10' group by a, b";

    public SqlToRelConverterToolTest() throws SqlParseException {}

    @Test
    public void test() throws SqlParseException {
        SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql);
        RelRoot relRoot = createSqlToRelConverter().convertQuery(sqlNode, true, true);
        Assert.assertNotNull(relRoot.rel);
        LOG.info(relRoot.rel.explain());
    }
}
