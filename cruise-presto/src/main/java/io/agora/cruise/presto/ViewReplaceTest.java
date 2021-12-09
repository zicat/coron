package io.agora.cruise.presto;

import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.SqlToRelConverter;

/** test. */
public class ViewReplaceTest extends PrestoQueryTest {

    public static void main(String[] args) throws SqlParseException {

        final CalciteContext context = new PrestoContext();
        context.addMaterializedView("test_view", viewQuery);
        final SqlNode sqlNode =
                SqlNodeTool.toQuerySqlNode(query, new Int2BooleanConditionShuttle());
        final SqlToRelConverter converter = context.createSqlToRelConverter();
        final RelRoot relRoot = converter.convertQuery(sqlNode, true, true);
        final RelNode optRelNode = context.materializedViewOpt(relRoot.rel);
        System.out.println(context.toSql(optRelNode));
    }

    static String viewQuery = "";
    static String query = "";
}
