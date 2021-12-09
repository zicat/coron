package io.agora.cruise.presto;

import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

/** test. */
public class ViewReplaceTest extends PrestoQueryTest {

    public static void main(String[] args) throws SqlParseException {

        final CalciteContext context = new PrestoContext();
        context.addMaterializedView("test_view", viewQuery);
        final SqlNode sqlNode =
                SqlNodeTool.toQuerySqlNode(query, new Int2BooleanConditionShuttle());
        final RelNode relNode = context.sqlNode2RelNode(sqlNode);
        final RelNode optRelNode = context.materializedViewOpt(relNode);
        System.out.println(context.toSql(optRelNode));
    }

    static String viewQuery = "";
    static String query = "";
}
