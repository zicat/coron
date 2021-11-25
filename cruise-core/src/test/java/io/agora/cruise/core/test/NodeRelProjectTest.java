package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import static io.agora.cruise.core.Node.findSubNode;
import static io.agora.cruise.core.NodeRel.createNodeRelRoot;

/** NodeRelProjectTest. */
public class NodeRelProjectTest extends NodeRelTest {

    public NodeRelProjectTest() throws SqlParseException {}

    @Test
    public void testProject() throws SqlParseException {

        String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        String sql2 = "SELECT a, b  FROM test_db.test_table";
        String expectSql = "SELECT a, b, ABS(a) c\nFROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject2() throws SqlParseException {

        String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        String sql2 = "SELECT c, b  FROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(similar.isEmpty());
    }

    @Test
    public void testProject3() throws SqlParseException {

        String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        String sql2 = "SELECT abs(d) as c, b  FROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(similar.isEmpty());
    }

    @Test
    public void testProject4() throws SqlParseException {

        String sql1 = "SELECT abs(a) as c, b FROM test_db.test_table";
        String sql2 = "SELECT abs(a) as c, b FROM test_db.test_table";
        String expectSql = "SELECT ABS(a) c, b\nFROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));

        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject5() throws SqlParseException {

        String sql1 = "SELECT * FROM test_db.test_table";
        String sql2 = "SELECT abs(a) as x, b FROM test_db.test_table";
        String expectSql = "SELECT ABS(a) x, b, c, d, a\nFROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));

        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testProject6() throws SqlParseException {

        String sql1 = "SELECT abs(a) as f, abs(b) as g FROM test_db.test_table";
        String sql2 = "SELECT abs(b) as f, abs(a) as g FROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));

        Assert.assertTrue(similar.isEmpty());
    }

    @Test
    public void testProject7() throws SqlParseException {

        String sql1 = "SELECT a as f, b as g FROM test_db.test_table";
        String sql2 = "SELECT * FROM test_db.test_table";
        String expectSql = "SELECT c, d, a, b, a f, b g\nFROM test_db.test_table";

        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }
}
