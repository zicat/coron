package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findFirstSubNode;

/** NodeRelProjectTest. */
public class NodeRelProjectTest extends NodeRelTest {

    public NodeRelProjectTest() throws SqlParseException {}

    @Test
    public void testProject() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT a, b  FROM test_db.test_table";
        final String expectSql = "SELECT a, b, ABS(a) c\nFROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject2() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT c, b  FROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testProject3() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT abs(d) as c, b  FROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());
        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testProject4() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String sql2 = "SELECT abs(a) as c, b FROM test_db.test_table";
        final String expectSql = "SELECT b, ABS(a) c\nFROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject5() throws SqlParseException {

        final String sql1 = "SELECT * FROM test_db.test_table";
        final String sql2 = "SELECT abs(a) as x, b FROM test_db.test_table";
        final String expectSql = "SELECT a, b, c, d, ABS(a) x\nFROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject6() throws SqlParseException {

        final String sql1 = "SELECT abs(a) as f, abs(b) as g FROM test_db.test_table";
        final String sql2 = "SELECT abs(b) as f, abs(a) as g FROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testProject7() throws SqlParseException {

        final String sql1 = "SELECT a as f, b as g FROM test_db.test_table";
        final String sql2 = "SELECT * FROM test_db.test_table";
        final String expectSql = "SELECT a, b, c, d, a f, b g\nFROM test_db.test_table";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject8() throws SqlParseException {
        final String sql1 =
                "select * from(select a, b "
                        + ",row_number() over (partition by a order by b asc) as row_num"
                        + ",max(c) over (partition by a)  as max_c "
                        + "from test_db.test_table) t where t.row_num = 1";

        final String sql2 =
                "select * from(select a, b "
                        + ",row_number() over (partition by a order by b asc) as row_num"
                        + ",max(c) over (partition by a)  as max_c "
                        + "from test_db.test_table) t where t.row_num <= 2";

        final String expectSql =
                "SELECT *\n"
                        + "FROM (SELECT a, b, MAX(c) OVER (PARTITION BY a RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) max_c, ROW_NUMBER() OVER (PARTITION BY a ORDER BY b NULLS LAST) row_num\n"
                        + "FROM test_db.test_table) t\n"
                        + "WHERE row_num = 1 OR row_num <= 2";

        final SqlNode sqlNode1 =
                SqlNodeTool.toSqlNode(sql1, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final SqlNode sqlNode2 =
                SqlNodeTool.toSqlNode(sql2, SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);
        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);
    }
}
