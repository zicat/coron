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

        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, c as dd, sum(d) from test_db.test_table WHERE c < 1000 group by a, c";

        final String expectSql1 =
                "SELECT a, c, d, b\nFROM test_db.test_table\nWHERE c < 1000 OR c < 5000";
        final String expectSql2 =
                "SELECT a, b, c, d\nFROM test_db.test_table\nWHERE c < 5000 OR c < 1000";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql1, resultNode);

        similar = findSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        resultNode = oneResultCheck(similar);
        assertResultNode(expectSql2, resultNode);
    }

    @Test
    public void testGroupBy2() throws SqlParseException {
        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(d) from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql1 =
                "SELECT a, b, d, c\nFROM test_db.test_table\nWHERE c < 1000 OR c < 5000";
        final String expectSql2 =
                "SELECT a, b, c, d\nFROM test_db.test_table\nWHERE c < 5000 OR c < 1000";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql1, resultNode);

        similar = findSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        resultNode = oneResultCheck(similar);
        assertResultNode(expectSql2, resultNode);
    }

    @Test
    public void testGroupBy3() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) as p from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, max(c) as t from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql1 =
                "SELECT a cc, b dd, MAX(c) t, a aa, b bb, SUM(c) p\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 1000 OR c < 5000\n"
                        + "GROUP BY a, b";
        final String expectSql2 =
                "SELECT a aa, b bb, SUM(c) p, a cc, b dd, MAX(c) t\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 5000 OR c < 1000\n"
                        + "GROUP BY a, b";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql1, resultNode);

        similar = findSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        resultNode = oneResultCheck(similar);
        assertResultNode(expectSql2, resultNode);
    }

    @Test
    public void testGroupBy4() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(c) from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql =
                "SELECT a cc, b dd, SUM(c), a aa, b bb\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 1000 OR c < 5000\n"
                        + "GROUP BY a, b";
        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        final ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        final ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testGroupBy5() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) as p from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b as dd, sum(c) as t from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql =
                "SELECT a cc, b dd, SUM(c) t, a aa, b bb, SUM(c) p\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 1000 OR c < 5000\n"
                        + "GROUP BY a, b";
        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        final ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        final ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void testGroupBy6() throws SqlParseException {

        final String sql1 =
                "select a as aa, sum(c) as p from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 =
                "select a as cc, b, sum(c) as t from test_db.test_table WHERE c < 1000 group by a, b";
        final String expectSql =
                "SELECT a cc, b, SUM(c) t, a aa, SUM(c) p\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 1000 OR c < 5000\n"
                        + "GROUP BY a, b";
        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        final ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        final ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }
}
