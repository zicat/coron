package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findSubNode;

/** NodeRelScanWithFilterTest. */
public class NodeRelScanWithFilterTest extends NodeRelTest {

    public NodeRelScanWithFilterTest() throws SqlParseException {}

    @Test
    public void test() throws SqlParseException {

        final String sql1 = "SELECT a as s, b AS aaa FROM test_db.test_table WHERE c < 5000";
        final String sql2 = "SELECT a, b, c  FROM test_db.test_table";
        final String expectSql = "SELECT a, b aaa, b, c, a s\nFROM test_db.test_table";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);

        similar = findSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }

    @Test
    public void test2() throws SqlParseException {

        final String sql1 =
                "select a as aa, b as bb, sum(c) as t from test_db.test_table WHERE c < 5000 group by a, b";
        final String sql2 = "select a as cc, b as dd, sum(d) from test_db.test_table group by a, b";
        final String expectSql =
                "SELECT SUM(d), a aa, b bb, a cc, b dd, SUM(c) t\n"
                        + "FROM test_db.test_table\n"
                        + "GROUP BY a, b";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNodeList<RelNode> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        ResultNode<RelNode> resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);

        similar = findSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        resultNode = oneResultCheck(similar);
        assertResultNode(expectSql, resultNode);
    }
}
