package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.ResultNodeList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;

/** NodeRelTest. */
public class NodeRelTest extends TestBase {

    public NodeRelTest() throws SqlParseException {}

    protected ResultNode<RelNode> oneResultCheck(ResultNodeList<RelNode> similar) {
        Assert.assertEquals(1, similar.size());
        return similar.get(0);
    }

    protected void assertResultNode(String expectSql, ResultNode<RelNode> resultNode) {
        Assert.assertEquals(expectSql, toSql(resultNode));
    }

    protected String toSql(ResultNode<RelNode> resultNode) {
        return toSql(resultNode.getPayload());
    }

    protected String toSql(RelNode relNode) {
        return relNode2SqlNode(relNode).toSqlString(SparkSqlDialect.DEFAULT).getSql();
    }
}
