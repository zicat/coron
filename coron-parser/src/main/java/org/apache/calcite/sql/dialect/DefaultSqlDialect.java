/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
     * @param context context
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
