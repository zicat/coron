package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;

/** NodeRelTest. */
public class NodeRelTest extends TestBase {

    public NodeRelTest() throws SqlParseException {}

    public NodeRelTest(String defaultDBName) throws SqlParseException {
        super(defaultDBName);
    }

    public void assertResultNode(String expectSql, ResultNode<RelNode> resultNode) {
        Assert.assertEquals(expectSql, toSql(resultNode));
    }

    public String toSql(ResultNode<RelNode> resultNode) {
        return toSql(resultNode.getPayload());
    }

    public String toSql(RelNode relNode) {
        return relNode2SqlNode(relNode).toSqlString(SparkSqlDialect.DEFAULT).getSql();
    }
}
