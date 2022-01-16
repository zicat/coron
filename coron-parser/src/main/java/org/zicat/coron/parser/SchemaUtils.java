/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.parser;

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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.Frameworks;
import org.zicat.coron.parser.util.CoronParserException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.SqlKind.CREATE_TABLE;

/** SchemaTool. */
public class SchemaUtils {

    /**
     * create schema plus from ddl list.
     *
     * @param rootSchema rootSchema
     * @param validator validator
     * @param defaultDBName defaultDBName
     * @param ddlList dbList
     * @throws SqlParseException SqlParseException
     */
    public static SchemaPlus addTableByDDL(
            SchemaPlus rootSchema,
            SqlValidator validator,
            SqlParser.Config config,
            String defaultDBName,
            String... ddlList)
            throws SqlParseException {

        if (ddlList == null || ddlList.length == 0) {
            throw new IllegalArgumentException("not support create schema from empty ddl list");
        }
        for (String ddl : ddlList) {
            SqlNode node = SqlNodeUtils.toSqlNode(ddl, config);
            if (node.getKind() != CREATE_TABLE) {
                throw new CoronParserException(
                        "register table only support ddl statement, sql:" + ddl);
            }
            SqlCreateTable createTable = (SqlCreateTable) node;
            addTable(
                    createTable.name.names,
                    defaultDBName,
                    rootSchema,
                    createColumnTable(createTable.columnList, validator));
        }
        return rootSchema;
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
            throw new IllegalArgumentException("register table name identifier length > 2");
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
                Boolean nullable = declaration.dataType.getNullable();
                nullable = nullable != null && nullable;
                RelDataType relDataType = declaration.dataType.deriveType(validator, nullable);
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
