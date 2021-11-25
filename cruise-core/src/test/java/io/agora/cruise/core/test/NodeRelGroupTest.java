package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

import static io.agora.cruise.core.Node.findSubNode;
import static io.agora.cruise.core.NodeRel.createNodeRelRoot;

/** NodeRelGroupTest. */
public class NodeRelGroupTest extends NodeRelTest {

    public NodeRelGroupTest() throws SqlParseException {}

    @Test
    public void testGroupBy() throws SqlParseException {

        String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        String sql2 =
                "select a as cc, c as dd, sum(d) from test_db.test_table WHERE c < 1000 group by a, c";
        String expectSql =
                "SELECT a, c, d, b\n" + "FROM test_db.test_table\n" + "WHERE c < 1000 OR c < 5000";

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
    public void testGroupBy2() throws SqlParseException {

        String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        String sql2 =
                "select a as cc, b as dd, sum(d) from test_db.test_table WHERE c < 1000 group by a, b";
        String expectSql =
                "SELECT a, b, d, c\n" + "FROM test_db.test_table\n" + "WHERE c < 1000 OR c < 5000";
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
    public void testGroupBy3() throws SqlParseException {

        String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        String sql2 =
                "select a as cc, b as dd, max(c) from test_db.test_table WHERE c < 1000 group by a, b";
        String expectSql =
                "SELECT a, b, c\n" + "FROM test_db.test_table\n" + "WHERE c < 1000 OR c < 5000";
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
    public void testGroupBy4() throws SqlParseException {

        String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        String sql2 =
                "select a as cc, b as dd, sum(c) from test_db.test_table WHERE c < 1000 group by a, b";
        String expectSql =
                "SELECT a cc, b dd, SUM(c), a aa, b bb\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 1000 OR c < 5000\n"
                        + "GROUP BY a, b";
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
