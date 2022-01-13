package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.DefaultSqlDialect;
import org.junit.Assert;

import java.util.Set;

/** NodeRelTest. */
public class NodeRelTest extends TestBase {

    public NodeRelTest() {}

    public void assertResultNode(String expectSql, ResultNode<RelNode> resultNode) {
        Assert.assertEquals(expectSql, toSql(resultNode));
    }

    public String toSql(ResultNode<RelNode> resultNode) {
        return relNode2SqlNode(resultNode.getPayload())
                .toSqlString(DefaultSqlDialect.DEFAULT)
                .getSql();
    }

    public void assertMaterialized(
            String materializeViewName, ResultNode<RelNode> resultNode, RelNode node2Opt) {
        addMaterializedView(materializeViewName, resultNode.getPayload());
        final RelNode optRelNode1 = materializedViewOpt(node2Opt).left;
        final Set<String> optTableNames = tables(optRelNode1);
        final String fullMaterializeViewName = defaultDatabase() + "." + materializeViewName;
        Assert.assertTrue(optTableNames.contains(fullMaterializeViewName));
    }

    public String dynamicViewName() {
        Throwable t = new Throwable();
        StackTraceElement[] st = t.getStackTrace();
        return "view_" + st[1].getMethodName();
    }
}
