package io.agora.cruise.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.agora.cruise.parser.sql.type.UTF16JavaTypeFactoryImp;
import io.agora.cruise.parser.sql.type.UTF16SqlTypeFactoryImpl;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.rules.materialize.AliasMaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.rules.materialize.AliasMaterializedViewOnlyJoinRule;
import org.apache.calcite.rel.rules.materialize.AliasMaterializedViewProjectAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.DefaultSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.CruiseSqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Frameworks;

import java.util.*;

import static io.agora.cruise.parser.sql.function.FunctionUtils.sqlOperatorTable;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

/** CalciteContext. */
public class CalciteContext {

    public static final Properties PROPERTIES = new Properties();
    public static final SqlTypeFactoryImpl DEFAULT_SQL_TYPE_FACTORY =
            new UTF16SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    public static final List<RelOptRule> MATERIALIZATION_RULES =
            ImmutableList.of(
                    MaterializedViewRules.FILTER_SCAN,
                    MaterializedViewRules.PROJECT_FILTER,
                    MaterializedViewRules.FILTER,
                    MaterializedViewRules.PROJECT_JOIN,
                    AliasMaterializedViewOnlyJoinRule.Config.DEFAULT.toRule(),
                    AliasMaterializedViewProjectAggregateRule.Config.DEFAULT.toRule(),
                    AliasMaterializedViewOnlyAggregateRule.Config.DEFAULT.toRule());

    static {
        PROPERTIES.put("caseSensitive", "false");
    }

    protected SchemaPlus rootSchema;
    protected final String defaultDatabase;
    protected final Map<String, List<RelOptMaterialization>> materializationMap = new HashMap<>();

    public CalciteContext(String defaultDatabase) {
        rootSchema = Frameworks.createRootSchema(true);
        this.defaultDatabase = defaultDatabase;
    }

    public CalciteContext() {
        this(SchemaTool.DEFAULT_DB_NAME);
    }

    /**
     * get default database.
     *
     * @return defaultDatabase
     */
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    /**
     * add table.
     *
     * @param ddlList ddl list
     * @return this
     * @throws SqlParseException exception.
     */
    public CalciteContext addTables(String... ddlList) throws SqlParseException {
        final SqlValidator validator = createValidator();
        rootSchema = SchemaTool.addTableByDDL(rootSchema, validator, defaultDatabase, ddlList);
        return this;
    }

    /**
     * add materializedView.
     *
     * @param viewName viewName
     * @param querySql query Sql
     * @return this
     * @throws SqlParseException SqlParseException
     */
    public CalciteContext addMaterializedView(String viewName, String querySql)
            throws SqlParseException {
        return addMaterializedView(viewName, querySql, new SqlShuttle[] {});
    }

    /**
     * add materializedView.
     *
     * @param viewName viewName
     * @param querySql query Sql
     * @param sqlShuttles sqlShuttles
     * @return this
     * @throws SqlParseException SqlParseException
     */
    public CalciteContext addMaterializedView(
            String viewName, String querySql, SqlShuttle... sqlShuttles) throws SqlParseException {
        final SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(querySql, sqlShuttles);
        final RelNode viewQueryRoot = sqlNode2RelNode(sqlNode);
        return addMaterializedView(viewName, viewQueryRoot);
    }

    /**
     * add materializedView.
     *
     * @param viewName viewName
     * @param viewQueryRel viewQueryRel
     * @param converter converter
     * @return this
     */
    public CalciteContext addMaterializedView(
            String viewName, RelNode viewQueryRel, SqlToRelConverter converter) {
        final ImmutableList<String> viewPath = ImmutableList.copyOf(viewName.split("\\."));
        SchemaTool.addTable(
                viewPath, defaultDatabase, rootSchema, new SchemaTool.RelTable(viewQueryRel));
        final Set<String> queryTables = TableRelShuttleImpl.tables(viewQueryRel);
        final RelNode tableReq =
                converter.toRel(createCatalogReader().getTable(viewPath), Lists.newArrayList());
        final RelOptMaterialization materialization =
                new RelOptMaterialization(
                        castNonNull(tableReq), castNonNull(viewQueryRel), null, viewPath);
        for (String queryTable : queryTables) {
            List<RelOptMaterialization> viewList =
                    materializationMap.computeIfAbsent(queryTable, k -> new ArrayList<>());
            viewList.add(materialization);
        }
        return this;
    }

    /**
     * add materializedView.
     *
     * @param viewName viewName
     * @param viewQueryRel viewQueryRel
     * @return this
     */
    public CalciteContext addMaterializedView(String viewName, RelNode viewQueryRel) {
        return addMaterializedView(viewName, viewQueryRel, createSqlToRelConverter());
    }

    /**
     * materialized query.
     *
     * @param relNode query node
     * @return opt node
     */
    public RelNode materializedViewOpt(RelNode relNode) {
        final Set<String> tables = TableRelShuttleImpl.tables(relNode);
        final HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
        MATERIALIZATION_RULES.forEach(builder::addRuleInstance);
        final HepProgram hepProgram = builder.build();
        final HepPlanner hepPlanner = new HepPlanner(hepProgram);
        for (String table : tables) {
            final List<RelOptMaterialization> matchResult = materializationMap.get(table);
            if (matchResult != null) {
                matchResult.forEach(hepPlanner::addMaterialization);
            }
        }
        hepPlanner.setRoot(relNode);
        return hepPlanner.findBestExp();
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
    private SqlToRelConverter createSqlToRelConverter() {
        final CalciteCatalogReader calciteCatalogReader = createCatalogReader();
        final HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
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
     * sqlNode to relNode.
     *
     * @param sqlNode sqlNode
     * @return relNode
     */
    public RelNode sqlNode2RelNode(SqlNode sqlNode) {
        final SqlToRelConverter converter = createSqlToRelConverter();
        final RelRoot viewQueryRoot = converter.convertQuery(sqlNode, true, true);
        final HepProgramBuilder builder =
                new HepProgramBuilder()
                        .addRuleInstance(FilterAggregateTransposeRule.Config.DEFAULT.toRule())
                        .addRuleInstance(FilterProjectTransposeRule.Config.DEFAULT.toRule())
                        .addRuleInstance(
                                AggregateProjectPullUpConstantsRule.Config.DEFAULT.toRule())
                        .addRuleInstance(ProjectMergeRule.Config.DEFAULT.toRule())
                        .addRuleInstance(FilterMergeRule.Config.DEFAULT.toRule());
        final HepPlanner hepPlanner = new HepPlanner(builder.build());
        hepPlanner.setRoot(viewQueryRoot.rel);
        return hepPlanner.findBestExp();
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
     * relNode to sql.
     *
     * @param relNode relNode
     * @return sql
     */
    public String toSql(RelNode relNode) {
        return toSql(relNode, DefaultSqlDialect.DEFAULT);
    }

    /**
     * relNode to sql.
     *
     * @param relNode relNode
     * @param sqlDialect sqlDialect
     * @return sql
     */
    public String toSql(RelNode relNode, SqlDialect sqlDialect) {
        return relNode2SqlNode(relNode).toSqlString(sqlDialect).getSql();
    }

    /**
     * @param relNode relNode
     * @return set tables
     */
    public Set<String> tables(RelNode relNode) {
        return TableRelShuttleImpl.tables(relNode);
    }

    /**
     * relNode toSqlNode.
     *
     * @param relNode relNode.
     * @return SqlNode
     */
    public SqlNode relNode2SqlNode(RelNode relNode) {
        return relNode2SqlNode(relNode, DefaultSqlDialect.DEFAULT);
    }

    /**
     * create relNode by sql.
     *
     * @param sql sql
     * @param sqlShuttles sqlShuttles
     * @return relNode
     * @throws SqlParseException SqlParseException
     */
    public RelNode querySql2Rel(String sql, SqlShuttle... sqlShuttles) throws SqlParseException {
        SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(sql, sqlShuttles);
        return sqlNode2RelNode(sqlNode);
    }

    /**
     * relNode toSqlNode.
     *
     * @param relNode relNode
     * @param sqlDialect sqlDialect
     * @return SqlNode
     */
    public SqlNode relNode2SqlNode(RelNode relNode, SqlDialect sqlDialect) {
        RelToSqlConverter relToSqlConverter =
                new JdbcImplementor(sqlDialect, new UTF16JavaTypeFactoryImp());
        SqlImplementor.Result result = relToSqlConverter.visitRoot(relNode);
        return result.asQueryOrValues();
    }
}
