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

    public SqlToRelConverterToolTest() throws SqlParseException {}

    @Test
    public void test() throws SqlParseException {
        final String querySql =
                "SELECT a, b, sum(c) from test_db.test_table where a > '10' group by a, b";
        final SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql);
        final RelRoot relRoot = createSqlToRelConverter().convertQuery(sqlNode, true, true);
        Assert.assertNotNull(relRoot.rel);
        LOG.info(relRoot.rel.explain());
    }

    @Test
    public void testUdf() throws SqlParseException {
        final String querySql =
                "SELECT a, my_udf(b), collection_distinct(c) from test_db.test_table where a > '10' group by a, b";
        final SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql);
        final RelRoot relRoot = createSqlToRelConverter().convertQuery(sqlNode, true, true);
        Assert.assertNotNull(relRoot.rel);
        LOG.info(relRoot.rel.explain());
    }

    @Test
    public void testOverWindow() throws SqlParseException {
        final String querySql =
                "select * from(select a, b "
                        + ",row_number() over (partition by a order by b asc) as row_num"
                        + ",max(c) over (partition by a)  as max_c "
                        + "from test_db.test_table) t where t.row_num = 1";
        final SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql);
        final RelRoot relRoot = createSqlToRelConverter().convertQuery(sqlNode, true, true);
        Assert.assertNotNull(relRoot.rel);
        LOG.info(relRoot.rel.explain());
    }

    @Test
    public void testDistinct() throws SqlParseException {
        final String querySql =
                "select a, count(distinct if(c > 0, b, a)) as c1 from test_db.test_table group by a";
        final SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql);
        final RelRoot relRoot = createSqlToRelConverter().convertQuery(sqlNode, true, true);
        Assert.assertNotNull(relRoot.rel);
        LOG.info(relRoot.rel.explain());
    }
}
