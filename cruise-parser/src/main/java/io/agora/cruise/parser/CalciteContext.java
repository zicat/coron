package io.agora.cruise.parser;

import io.agora.cruise.parser.sql.type.UTF16SqlTypeFactoryImpl;
import io.agora.cruise.parser.sql.validate.CruiseSqlValidatorImpl;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Frameworks;

import java.util.Properties;

import static io.agora.cruise.parser.sql.function.FunctionUtils.sqlOperatorTable;

/** CalciteContext. */
public class CalciteContext {

    public static final Properties PROPERTIES = new Properties();
    public static final SqlTypeFactoryImpl DEFAULT_SQL_TYPE_FACTORY =
            new UTF16SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    static {
        PROPERTIES.put("caseSensitive", "false");
    }

    protected SchemaPlus rootSchema;

    public CalciteContext() {
        rootSchema = Frameworks.createRootSchema(true);
    }

    /**
     * add table.
     *
     * @param ddlList ddl list
     * @return this
     * @throws SqlParseException exception.
     */
    public CalciteContext addTables(String... ddlList) throws SqlParseException {
        SqlValidator validator = createValidator();
        SchemaTool.addTableByDDL(rootSchema, validator, ddlList);
        return this;
    }

    /**
     * add function.
     *
     * @param name name
     * @param function function
     * @return CalciteContext
     */
    public CalciteContext addFunction(String name, Function function) {
        rootSchema.add(name, function);
        return this;
    }

    /**
     * create validator.
     *
     * @return SqlValidator
     */
    protected SqlValidator createValidator() {
        return CruiseSqlValidatorImpl.newValidator(
                sqlTypeFactory(),
                createSqlOperatorTable(),
                createCatalogReader(),
                sqlTypeFactory(),
                SqlValidator.Config.DEFAULT
                        .withLenientOperatorLookup(true)
                        .withColumnReferenceExpansion(true)
                        .withSqlConformance(SqlConformanceEnum.LENIENT));
    }

    /**
     * Create SqlOperatorTable.
     *
     * @return SqlOperatorTable
     */
    protected SqlOperatorTable createSqlOperatorTable() {
        return sqlOperatorTable;
    }

    /**
     * create SqlToRelConverter.
     *
     * @return SqlToRelConverter
     */
    public SqlToRelConverter createSqlToRelConverter() {
        final CalciteCatalogReader calciteCatalogReader = createCatalogReader();
        final HepPlanner hepPlanner = createPlanner();
        final SqlValidator validator = createValidator();
        return SqlToRelConverterTool.createSqlToRelConverter(
                hepPlanner,
                sqlTypeFactory(),
                calciteCatalogReader,
                createSqlOperatorTable(),
                rootSchema,
                validator,
                null);
    }

    /**
     * Create CalciteCatalogReader.
     *
     * @return CalciteCatalogReader
     */
    protected CalciteCatalogReader createCatalogReader() {
        CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
        return new CalciteCatalogReader(
                calciteSchema,
                calciteSchema.path(null),
                sqlTypeFactory(),
                new CalciteConnectionConfigImpl(PROPERTIES));
    }

    /**
     * Create SqlTypeFactoryImpl.
     *
     * @return SqlTypeFactoryImpl
     */
    public SqlTypeFactoryImpl sqlTypeFactory() {
        return DEFAULT_SQL_TYPE_FACTORY;
    }

    /**
     * create planner.
     *
     * @return
     */
    protected HepPlanner createPlanner() {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
        HepProgram hepProgram = builder.build();
        return new HepPlanner(hepProgram);
    }
}
