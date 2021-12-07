package io.agora.cruise.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.Frameworks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.CREATE_TABLE;

/** SchemaTool. */
public class SchemaTool {

    public static final String DEFAULT_DB_NAME = "default";

    /**
     * create schema plus from ddl list.
     *
     * @param rootSchema rootSchema
     * @param validator validator
     * @param defaultDBName defaultDBName
     * @param ddlList dbList
     * @return rootSchema
     * @throws SqlParseException SqlParseException
     */
    public static SchemaPlus addTableByDDL(
            SchemaPlus rootSchema, SqlValidator validator, String defaultDBName, String... ddlList)
            throws SqlParseException {
        if (ddlList == null || ddlList.length == 0) {
            throw new IllegalArgumentException("not support create schema from empty ddl list");
        }
        for (String ddl : ddlList) {
            SqlNode node = SqlNodeTool.toDDLSqlNode(ddl);
            if (node.getKind() != CREATE_TABLE) {
                throw new RuntimeException("register table only support ddl statement, sql:" + ddl);
            }
            SqlCreateTable createTable = (SqlCreateTable) node;
            ImmutableList<String> fullName = createTable.name.names;
            if (fullName.size() > 2) {
                throw new RuntimeException("register table name identifier length > 2");
            }
            String dbName = fullName.size() == 2 ? fullName.get(0) : DEFAULT_DB_NAME;
            String tableName = fullName.get(fullName.size() - 1);
            SchemaPlus schemaPlus = rootSchema.getSubSchema(dbName);
            if (schemaPlus == null) {
                schemaPlus = Frameworks.createRootSchema(false);
                rootSchema.add(dbName, schemaPlus);
            }
            Table table = createColumnTable(createTable.columnList, validator);
            schemaPlus.add(tableName, table);
            if (defaultDBName.equals(dbName)) {
                rootSchema.add(tableName, table);
            }
        }
        return rootSchema;
    }

    /**
     * create column table by sql node list.
     *
     * @param sqlNodeList sql node list
     * @return column table
     */
    private static ColumnTable createColumnTable(SqlNodeList sqlNodeList, SqlValidator validator) {
        return new ColumnTable(
                Objects.requireNonNull(sqlNodeList).stream()
                        .map(c -> (SqlColumnDeclaration) c)
                        .collect(Collectors.toList()),
                validator);
    }

    /** ColumnTable. */
    static class ColumnTable extends AbstractTable {

        private final List<SqlColumnDeclaration> columnList;
        private final SqlValidator validator;

        public ColumnTable(List<SqlColumnDeclaration> columnList, SqlValidator validator) {
            this.columnList = columnList;
            this.validator = validator;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {

            List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();
            for (SqlColumnDeclaration declaration : columnList) {
                RelDataType relDataType = declaration.dataType.deriveType(validator, true);
                fieldList.add(Maps.immutableEntry(declaration.name.getSimple(), relDataType));
            }
            return typeFactory.createStructType(fieldList);
        }
    }
}
