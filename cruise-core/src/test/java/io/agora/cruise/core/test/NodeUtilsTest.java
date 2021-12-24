package io.agora.cruise.core.test;

import io.agora.cruise.core.NodeUtils;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

/** NodeUtilsTest. */
public class NodeUtilsTest extends NodeRelTest {

    public NodeUtilsTest() throws SqlParseException {}

    @Test
    public void test() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join (select * from test_db.test_table where a in (111, 222)) t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        Assert.assertEquals(5, NodeUtils.deep(relNode1));
    }
}
