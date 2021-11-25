package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Test;

import java.util.List;

import static io.agora.cruise.core.Node.findSubNode;
import static io.agora.cruise.core.NodeRel.createNodeRelRoot;

/** NodeRelTest. */
public class NodeRelTest extends TestBase {

    public NodeRelTest() throws SqlParseException {}

    String sql1 =
            "SELECT a as s, b AS aaa FROM test_db.test_table WHERE c < 5000 UNION ALL SELECT g, h FROM test_db.test_table2";
    String sql2 = "SELECT a, b  FROM test_db.test_table WHERE c < 1000";

    @Test
    public void test() throws SqlParseException {
        SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        List<ResultNode<RelNode>> similar =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));

        for (ResultNode<RelNode> node : similar) {

            SqlNode commonSqlNode = relNode2SqlNode(node.getPayload());
            LOG.info(
                    "\n=======Common Sub Node=========\n"
                            + commonSqlNode.toSqlString(SparkSqlDialect.DEFAULT).getSql());
        }
    }
}
