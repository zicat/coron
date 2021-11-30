package io.agora.cruise.core.test;

import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.sql.type.UTF16JavaTypeFactoryImp;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;

/** TestBase. */
public class TestBase extends CalciteContext {

    protected String ddl1 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table (c INT64, d INT64, a INT32, b INT32)";

    protected String ddl2 =
            "CREATE TABLE IF NOT EXISTS test_db.test_table2 (e INT64, f BIGINT, g INT32, h INT32)";

    public TestBase() throws SqlParseException {
        super();
        addTables(ddl1, ddl2);
    }

    protected SqlNode relNode2SqlNode(RelNode relNode) {
        RelToSqlConverter relToSqlConverter =
                new JdbcImplementor(SparkSqlDialect.DEFAULT, new UTF16JavaTypeFactoryImp());
        SqlImplementor.Result result = relToSqlConverter.visitRoot(relNode);
        return result.asQueryOrValues();
    }
}
