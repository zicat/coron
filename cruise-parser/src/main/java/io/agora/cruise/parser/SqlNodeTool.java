package io.agora.cruise.parser;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/** SqlParserConfigTool. */
public class SqlNodeTool {

    public static final SqlParser.Config DEFAULT_QUERY_PARSER_CONFIG =
            SqlParser.config()
                    .withLex(Lex.MYSQL)
                    .withCaseSensitive(false)
                    .withIdentifierMaxLength(128)
                    .withConformance(SqlConformanceEnum.LENIENT)
                    .withParserFactory(AgoraSqlParserImpl.FACTORY);

    public static final SqlParser.Config DEFAULT_DDL_PARSER_CONFIG =
            SqlParser.config()
                    .withLex(Lex.MYSQL)
                    .withCaseSensitive(false)
                    .withIdentifierMaxLength(128)
                    .withQuoting(Quoting.BACK_TICK)
                    .withConformance(SqlConformanceEnum.DEFAULT)
                    .withParserFactory(AgoraSqlParserImpl.FACTORY);

    public static SqlNode toQuerySqlNode(String sql) throws SqlParseException {
        SqlParser sqlParser = fromQuerySql(sql);
        return sqlParser.parseStmt();
    }

    public static SqlNode toDDLSqlNode(String sql) throws SqlParseException {
        SqlParser sqlParser = fromDDLSql(sql);
        return sqlParser.parseStmt();
    }

    private static SqlParser fromQuerySql(String sql) {
        return SqlParser.create(sql, DEFAULT_QUERY_PARSER_CONFIG);
    }

    private static SqlParser fromDDLSql(String sql) {
        return SqlParser.create(sql, DEFAULT_DDL_PARSER_CONFIG);
    }

    public static String toSql(SqlNode sqlNode) {
        return toSql(sqlNode, SparkSqlDialect.DEFAULT);
    }

    public static String toSql(SqlNode sqlNode, SqlDialect dialect) {
        return sqlNode.toSqlString(dialect).getSql();
    }
}
