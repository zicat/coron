package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.sql.dialect.DefaultSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;

import java.util.Set;

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
        return relNode2SqlNode(relNode).toSqlString(DefaultSqlDialect.DEFAULT).getSql();
    }

    public Set<String> tables(RelNode relNode) {
        return TableRelShuttleImpl.tables(relNode);
    }

    public void assertMaterialized(
            String materializeViewName, ResultNode<RelNode> resultNode, RelNode node2Opt) {
        addMaterializedView(materializeViewName, resultNode.getPayload());
        final RelNode optRelNode1 = materializedViewOpt(node2Opt);
        final Set<String> optTableNames = tables(optRelNode1);
        Assert.assertTrue(optTableNames.contains(materializeViewName));
    }

    public String dynamicViewName() {
        Throwable t = new Throwable();
        StackTraceElement[] st = t.getStackTrace();
        return "view_" + st[1].getMethodName();
    }
}
