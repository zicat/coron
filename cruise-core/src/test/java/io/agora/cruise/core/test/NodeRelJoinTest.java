package io.agora.cruise.core.test;

import io.agora.cruise.core.ResultNode;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findFirstSubNode;

/** NodeRelJoinTest. */
public class NodeRelJoinTest extends NodeRelTest {

    public NodeRelJoinTest() throws SqlParseException {}

    @Test
    public void testJoin() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final String sql2 =
                "select t1.a from test_db.test_table t1 "
                        + "left join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testJoin2() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String sql2 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.c ";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        Assert.assertTrue(resultNode.isEmpty());

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        Assert.assertTrue(resultNode.isEmpty());
    }

    @Test
    public void testJoin3() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final String sql2 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.a ";
        final String expectSql =
                "SELECT test_table.a\n"
                        + "FROM test_db.test_table\n"
                        + "INNER JOIN test_db.test_table test_table0 ON test_table.a = test_table0.a AND test_table.b = test_table0.a";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }

    @Test
    public void testJoin4() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String sql2 =
                "select t2.a as x from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String expectSql =
                "SELECT test_table.a, test_table0.a x\n"
                        + "FROM test_db.test_table\n"
                        + "INNER JOIN test_db.test_table test_table0 ON test_table.a = test_table0.a AND test_table.b = test_table0.b";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }

    @Test
    public void testJoin5() throws SqlParseException {

        final String sql1 =
                "select t1.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String sql2 =
                "select t2.a from test_db.test_table t1 "
                        + "inner join test_db.test_table t2 "
                        + "on t1.a = t2.a and t1.b=t2.b ";
        final String expectSql =
                "SELECT *\n"
                        + "FROM test_db.test_table\n"
                        + "INNER JOIN test_db.test_table test_table0 ON test_table.a = test_table0.a AND test_table.b = test_table0.b";

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = sqlNode2RelNode(sqlNode1);
        final RelNode relNode2 = sqlNode2RelNode(sqlNode2);

        ResultNode<RelNode> resultNode =
                findFirstSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);

        resultNode = findFirstSubNode(createNodeRelRoot(relNode2), createNodeRelRoot(relNode1));
        assertResultNode(expectSql, resultNode);

        assertMaterialized(dynamicViewName(), resultNode, relNode1);
        assertMaterialized(dynamicViewName(), resultNode, relNode2);
    }
}
