package org.apache.calcite.sql.dialect;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;

/** PrestoDialect. */
public class PrestoDialect extends DefaultSqlDialect {

    public static final SqlDialect.Context DEFAULT_CONTEXT =
            SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(SqlDialect.DatabaseProduct.SPARK)
                    .withIdentifierQuoteString("\"")
                    .withNullCollation(NullCollation.LOW);

    public static final SqlDialect DEFAULT = new PrestoDialect(DEFAULT_CONTEXT);

    /**
     * Creates a SparkSqlDialect.
     *
     * @param context context
     */
    public PrestoDialect(Context context) {
        super(context);
    }

    @Override
    protected boolean allowsAs() {
        return true;
    }
}
