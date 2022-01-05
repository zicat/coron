package io.agora.cruise.parser.test;

import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/** MaterializedViewTest. */
public class MaterializedViewTest extends TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewTest.class);

    public MaterializedViewTest() {}

    @Test
    public void testView2() throws SqlParseException {
        String viewQuerySql = "select sum(c), count(*) as s_c from test_db.test_table";
        String viewTableName = "test_db.testView2";
        addMaterializedView(viewTableName, viewQuerySql);
        final SqlNode sqlNode =
                SqlNodeTool.toSqlNode(
                        "select sum(c) as s_c from test_db.test_table having count(*) > 1",
                        SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        final RelNode optNode = materializedViewOpt(relNode);
        final Set<String> queryTables = TableRelShuttleImpl.tables(optNode);
        Assert.assertTrue(queryTables.contains(viewTableName));
    }

    @Test
    public void testAddView() throws SqlParseException {
        String viewQuerySql = "select a, sum(c) as s_c from test_db.test_table group by a";
        String viewTableName = "test_db.materialized_view";
        addMaterializedView(viewTableName, viewQuerySql);
        String sql1 = "select * from test_db.materialized_view";
        final SqlNode sqlNode =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        Assert.assertNotNull(relNode);
        LOG.info(relNode.explain());

        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(viewQuerySql, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);
        final RelNode optRelNode = materializedViewOpt(relNode2);
        final Set<String> queryTables = TableRelShuttleImpl.tables(optRelNode);
        Assert.assertTrue(queryTables.contains(viewTableName));
    }

    @Test
    public void testView3() throws SqlParseException {
        String viewQuerySql = "select a,b,sum(c) from test_db.test_table group by a,b";
        String viewTableName = "test_db.testView2";
        addMaterializedView(viewTableName, viewQuerySql);
        final SqlNode sqlNode =
                SqlNodeTool.toSqlNode(
                        "select a,sum(c) from test_db.test_table group by a",
                        SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode = sqlNode2RelNode(sqlNode);
        final RelNode optNode = materializedViewOpt(relNode);
        final Set<String> queryTables = TableRelShuttleImpl.tables(optNode);
        Assert.assertTrue(queryTables.contains(viewTableName));
    }
}
