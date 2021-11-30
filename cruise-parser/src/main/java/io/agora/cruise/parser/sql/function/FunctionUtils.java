package io.agora.cruise.parser.sql.function;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.Optionality;

/** FunctionUtils. */
public class FunctionUtils {

    public static SqlOperatorTable sqlOperatorTable;

    static {
        ListSqlOperatorTable listSqlOperatorTable = new ListSqlOperatorTable();
        listSqlOperatorTable.add(new CollectionDistinct());
        sqlOperatorTable =
                SqlOperatorTables.chain(listSqlOperatorTable, SqlStdOperatorTable.instance());
    }

    /** CollectionDistinct. */
    public static class CollectionDistinct extends SqlAggFunction {

        public CollectionDistinct() {
            super(
                    "collection_distinct",
                    null,
                    SqlKind.ARRAY_AGG,
                    ReturnTypes.VARCHAR_2000,
                    null,
                    OperandTypes.family(SqlTypeFamily.ANY),
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false,
                    Optionality.OPTIONAL);
        }
    }
}
