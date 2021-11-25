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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Frameworks;

import java.util.Properties;

/** CalciteContext. */
public class CalciteContext {

    private static final Properties properties = new Properties();

    public static SqlTypeFactoryImpl sqlTypeFactory() {
        return new UTF16SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    }

    static {
        properties.put("caseSensitive", "false");
    }

    protected SqlTypeFactoryImpl factory = sqlTypeFactory();
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
    public CalciteContext addTable(String... ddlList) throws SqlParseException {
        SqlValidator validator = createValidator();
        SchemaTool.addTableByDDL(rootSchema, validator, ddlList);
        return this;
    }

    protected SqlValidator createValidator() {
        return CruiseSqlValidatorImpl.newValidator(
                SqlStdOperatorTable.instance(),
                createCatalogReader(),
                factory,
                SqlValidator.Config.DEFAULT
                        .withLenientOperatorLookup(true)
                        .withColumnReferenceExpansion(true)
                        .withSqlConformance(SqlConformanceEnum.LENIENT));
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
                hepPlanner, factory, calciteCatalogReader, rootSchema, validator, null);
    }

    protected CalciteCatalogReader createCatalogReader() {
        CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
        return new CalciteCatalogReader(
                calciteSchema,
                calciteSchema.path(null),
                factory,
                new CalciteConnectionConfigImpl(properties));
    }

    protected HepPlanner createPlanner() {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
        HepProgram hepProgram = builder.build();
        return new HepPlanner(hepProgram);
    }
}
