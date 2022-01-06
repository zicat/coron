package io.agora.cruise.parser;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/** SqlParserConfigTool. */
public class SqlNodeUtils {

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

    /**
     * Sql to query SqlNode.
     *
     * @param sql sql
     * @param config config
     * @param sqlShuttles sql shuttle
     * @return SqlNode
     * @throws SqlParseException SqlParseException
     */
    public static SqlNode toSqlNode(String sql, SqlParser.Config config, SqlShuttle... sqlShuttles)
            throws SqlParseException {
        SqlNode sqlNode = toSqlNode(sql, config);
        if (sqlShuttles != null && sqlShuttles.length > 0) {
            for (SqlShuttle sqlShuttle : sqlShuttles) {
                sqlNode = sqlNode.accept(sqlShuttle);
            }
        }
        return sqlNode;
    }

    /**
     * Sql to ddl SqlNode.
     *
     * @param sql sql
     * @param config config
     * @return SqlNode
     * @throws SqlParseException SqlParseException
     */
    public static SqlNode toSqlNode(String sql, SqlParser.Config config) throws SqlParseException {
        SqlParser sqlParser = fromSql(sql, config);
        return sqlParser.parseStmt();
    }

    /**
     * Sql to query SqlParser.
     *
     * @param sql sql
     * @return SqlParser
     */
    private static SqlParser fromSql(String sql, SqlParser.Config config) {
        return SqlParser.create(sql, config);
    }

    /**
     * SqlNode to sql, default spark dialect.
     *
     * @param sqlNode SqlNode
     * @return sql
     */
    public static String toSql(SqlNode sqlNode) {
        return toSql(sqlNode, SparkSqlDialect.DEFAULT);
    }

    /**
     * SqlNode to sql.
     *
     * @param sqlNode SqlNode
     * @param dialect dialect
     * @return sql
     */
    public static String toSql(SqlNode sqlNode, SqlDialect dialect) {
        return sqlNode.toSqlString(dialect).getSql();
    }
}
