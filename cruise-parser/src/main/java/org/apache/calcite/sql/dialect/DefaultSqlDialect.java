package org.apache.calcite.sql.dialect;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;

import javax.annotation.Nullable;

/** DefaultSqlDialect. */
public class DefaultSqlDialect extends SparkSqlDialect {

    public static final SqlDialect.Context DEFAULT_CONTEXT =
            SqlDialect.EMPTY_CONTEXT
                    .withDatabaseProduct(SqlDialect.DatabaseProduct.SPARK)
                    .withNullCollation(NullCollation.LOW);

    public static final SqlDialect DEFAULT = new DefaultSqlDialect(DEFAULT_CONTEXT);
    /**
     * Creates a SparkSqlDialect.
     *
     * @param context
     */
    public DefaultSqlDialect(Context context) {
        super(context);
    }

    @Override
    public void quoteStringLiteral(StringBuilder buf, @Nullable String charsetName, String val) {
        if (containsNonAscii(val) && charsetName == null) {
            buf.append(literalQuoteString);
            buf.append(val.replace(literalEndQuoteString, literalEscapedQuote));
            buf.append(literalEndQuoteString);
            return;
        }
        super.quoteStringLiteral(buf, charsetName, val);
    }
}
