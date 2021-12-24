package io.agora.cruise.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
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

    /**
     * create schema plus from ddl list.
     *
     * @param rootSchema rootSchema
     * @param validator validator
     * @param defaultDBName defaultDBName
     * @param ddlList dbList
     * @throws SqlParseException SqlParseException
     */
    public static void addTableByDDL(
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
            addTable(
                    createTable.name.names,
                    defaultDBName,
                    rootSchema,
                    createColumnTable(createTable.columnList, validator));
        }
    }

    /**
     * add table.
     *
     * @param fullName fullName
     * @param defaultDBName defaultDBName
     * @param rootSchema rootSchema
     * @param table table
     * @return schemaPlus
     */
    public static SchemaPlus addTable(
            ImmutableList<String> fullName,
            String defaultDBName,
            SchemaPlus rootSchema,
            Table table) {
        if (fullName.size() > 2) {
            throw new RuntimeException("register table name identifier length > 2");
        }
        String dbName = fullName.size() == 2 ? fullName.get(0) : defaultDBName;
        String tableName = fullName.get(fullName.size() - 1);
        SchemaPlus schemaPlus = rootSchema.getSubSchema(dbName);
        if (schemaPlus == null) {
            schemaPlus = Frameworks.createRootSchema(false);
            rootSchema.add(dbName, schemaPlus);
        }
        schemaPlus.add(tableName, table);
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

    /** RelTable. */
    static class RelTable extends AbstractTable {

        private final RelNode relNode;

        public RelTable(RelNode relNode) {
            this.relNode = relNode;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return relNode.getRowType();
        }
    }
}
