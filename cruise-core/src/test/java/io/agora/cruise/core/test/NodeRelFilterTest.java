package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

import static io.agora.cruise.core.NodeRel.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findSubNode;

/** NodeRelFilterTest. */
public class NodeRelFilterTest extends NodeRelTest {

    public NodeRelFilterTest() throws SqlParseException {}

    @Test
    public void testFilterProject() throws SqlParseException {

        final String sql1 =
                "SELECT a as s, b AS aaa FROM test_db.test_table WHERE c < 5000 UNION ALL SELECT g, h FROM test_db.test_table2";
        final String sql2 = "SELECT a, b, c  FROM test_db.test_table WHERE c < 1000";
        final String expectSql1 =
                "SELECT a, b, c, a s, b aaa\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 1000 OR c < 5000";
        final String expectSql2 =
                "SELECT a s, b aaa, a, b, c\n"
                        + "FROM test_db.test_table\n"
                        + "WHERE c < 5000 OR c < 1000";

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
}
