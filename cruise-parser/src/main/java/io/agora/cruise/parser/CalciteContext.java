package io.agora.cruise.parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.agora.cruise.parser.sql.function.FunctionUtils;
import io.agora.cruise.parser.sql.type.UTF16JavaTypeFactoryImp;
import io.agora.cruise.parser.util.LockUtils;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.rules.materialize.AliasMaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.rules.materialize.AliasMaterializedViewOnlyJoinRule;
import org.apache.calcite.rel.rules.materialize.AliasMaterializedViewProjectAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.dialect.DefaultSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.CruiseSqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Pair;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.agora.cruise.parser.SqlNodeUtils.DEFAULT_DDL_PARSER_CONFIG;
import static io.agora.cruise.parser.SqlNodeUtils.DEFAULT_QUERY_PARSER_CONFIG;
import static io.agora.cruise.parser.util.LockUtils.lock;
import static org.apache.calcite.linq4j.Nullness.castNonNull;

/** CalciteContext. @ThreadSafe */
public class CalciteContext {

    private static final Object VOID = new Object();
    public static final String DEFAULT_DB_NAME = "default";
    public static final SqlTypeFactoryImpl DEFAULT_SQL_TYPE_FACTORY = new UTF16JavaTypeFactoryImp();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    protected final SchemaPlus rootSchema;
    protected final CalciteSchema calciteSchema;
    protected final SqlTypeFactoryImpl typeFactory;
    protected final CalciteCatalogReader calciteCatalogReader;
    protected final String defaultDatabase;
    protected final Map<String, List<RelOptMaterialization>> materializationMap = new HashMap<>();
    protected final SqlOperatorTable sqlOperatorTable;
    protected final SqlParser.Config ddlConfig;
    protected final SqlParser.Config queryConfig;

    public CalciteContext(String defaultDatabase) {
        this.defaultDatabase = defaultDatabase;
        this.ddlConfig = defaultDDLConfig();
        this.queryConfig = defaultQueryConfig();
        this.rootSchema = defaultSchemaPlus();
        this.sqlOperatorTable = defaultSqlOperatorTable();
        this.calciteSchema = defaultCalciteSchema();
        this.typeFactory = defaultSqlTypeFactory();
        this.calciteCatalogReader = defaultCatalogReader();
    }

    public CalciteContext() {
        this(DEFAULT_DB_NAME);
    }

    /**
     * create default SchemaPlus.
     *
     * @return SchemaPlus
     */
    protected SchemaPlus defaultSchemaPlus() {
        return Frameworks.createRootSchema(true);
    }

    /**
     * create default ddl config.
     *
     * @return config
     */
    protected SqlParser.Config defaultDDLConfig() {
        return DEFAULT_DDL_PARSER_CONFIG;
    }

    /**
     * create default query config.
     *
     * @return config
     */
    protected SqlParser.Config defaultQueryConfig() {
        return DEFAULT_QUERY_PARSER_CONFIG;
    }

    /**
     * Create SqlOperatorTable.
     *
     * @return SqlOperatorTable
     */
    protected SqlOperatorTable defaultSqlOperatorTable() {
        return FunctionUtils.sqlOperatorTable;
    }

    /**
     * create default CalciteSchema.
     *
     * @return CalciteSchema
     */
    protected CalciteSchema defaultCalciteSchema() {
        return CalciteSchema.from(rootSchema);
    }

    /**
     * Create SqlTypeFactoryImpl.
     *
     * @return SqlTypeFactoryImpl
     */
    protected SqlTypeFactoryImpl defaultSqlTypeFactory() {
        return DEFAULT_SQL_TYPE_FACTORY;
    }

    /**
     * create CatalogReader.
     *
     * @return CalciteCatalogReader
     */
    protected CalciteCatalogReader defaultCatalogReader() {
        Properties properties = new Properties();
        properties.put("caseSensitive", "false");
        return new CalciteCatalogReader(
                calciteSchema,
                calciteSchema.path(defaultDatabase),
                typeFactory,
                new CalciteConnectionConfigImpl(properties));
    }

    /**
     * create validator.
     *
     * @return SqlValidator
     */
    protected SqlValidator createValidator() {
        return CruiseSqlValidatorImpl.newValidator(
                typeFactory,
                sqlOperatorTable,
                calciteCatalogReader,
                typeFactory,
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
    protected SqlToRelConverter createSqlToRelConverter(RelOptPlanner planner) {
        return SqlToRelConverterUtils.createSqlToRelConverter(
                planner,
                typeFactory,
                calciteCatalogReader,
                sqlOperatorTable,
                rootSchema,
                createValidator(),
                null);
    }

    /**
     * create default hep planner.
     *
     * @return defaultPlanner
     */
    protected HepPlanner defaultRelOptPlanner() {
        final HepProgramBuilder builder =
                new HepProgramBuilder()
                        .addRuleInstance(FilterAggregateTransposeRule.Config.DEFAULT.toRule())
                        .addRuleInstance(FilterProjectTransposeRule.Config.DEFAULT.toRule())
                        .addRuleInstance(
                                AggregateProjectPullUpConstantsRule.Config.DEFAULT.toRule())
                        .addRuleInstance(ProjectMergeRule.Config.DEFAULT.toRule())
                        .addRuleInstance(FilterMergeRule.Config.DEFAULT.toRule());
        return new HepPlanner(builder.build());
    }

    /**
     * create default defaultMaterializedHepPlanner.
     *
     * @return HepPlanner
     */
    protected HepPlanner createMaterializedHepPlanner() {
        final HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);
        ImmutableList.of(
                        MaterializedViewRules.FILTER_SCAN,
                        MaterializedViewRules.PROJECT_FILTER,
                        MaterializedViewRules.FILTER,
                        MaterializedViewRules.PROJECT_JOIN,
                        AliasMaterializedViewOnlyJoinRule.Config.DEFAULT.toRule(),
                        AliasMaterializedViewProjectAggregateRule.Config.DEFAULT.toRule(),
                        AliasMaterializedViewOnlyAggregateRule.Config.DEFAULT.toRule())
                .forEach(builder::addRuleInstance);
        final HepProgram hepProgram = builder.build();
        return new HepPlanner(hepProgram);
    }

    /**
     * get default database.
     *
     * @return defaultDatabase
     */
    public String defaultDatabase() {
        return defaultDatabase;
    }

    /**
     * add table.
     *
     * @param ddlList ddl list
     * @return this
     */
    public CalciteContext addTables(String... ddlList) {
        final SqlValidator validator = createValidator();
        writeLock(
                () ->
                        SchemaUtils.addTableByDDL(
                                rootSchema, validator, ddlConfig, defaultDatabase, ddlList));
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
        final SqlNode sqlNode = SqlNodeUtils.toSqlNode(querySql, queryConfig, sqlShuttles);
        final RelNode viewQueryRoot = sqlNode2RelNode(sqlNode);
        return addMaterializedView(viewName, viewQueryRoot);
    }

    /**
     * add materializedView.
     *
     * @param viewName viewName
     * @param viewQueryRel viewQueryRel
     * @return this
     */
    public CalciteContext addMaterializedView(String viewName, RelNode viewQueryRel) {

        final ImmutableList<String> viewPath = ImmutableList.copyOf(viewName.split("\\."));
        addTable(viewPath, defaultDatabase, rootSchema, viewQueryRel);
        final Set<String> queryTables = TableRelShuttleImpl.tables(viewQueryRel);
        final RelOptPlanner planner = defaultRelOptPlanner();
        final SqlToRelConverter converter = createSqlToRelConverter(planner);
        final Prepare.PreparingTable table =
                readLock(() -> calciteCatalogReader.getTable(viewPath));
        final RelNode tableReq = converter.toRel(table, Lists.newArrayList());
        final RelOptMaterialization materialization =
                new RelOptMaterialization(
                        castNonNull(tableReq), castNonNull(viewQueryRel), null, viewPath);
        addMaterializationCache(queryTables, materialization);
        return this;
    }

    /**
     * add materialization to cache.
     *
     * @param queryTables queryTables
     * @param materialization materialization
     */
    private void addMaterializationCache(
            Set<String> queryTables, RelOptMaterialization materialization) {
        writeLock(
                () -> {
                    for (String queryTable : queryTables) {
                        List<RelOptMaterialization> viewList =
                                materializationMap.computeIfAbsent(
                                        queryTable, k -> new ArrayList<>());
                        viewList.add(materialization);
                    }
                    return VOID;
                });
    }

    /**
     * add table with writeLock.
     *
     * @param fullName fullName
     * @param defaultDBName defaultDBName
     * @param rootSchema rootSchema
     * @param relNode relNode
     */
    private void addTable(
            ImmutableList<String> fullName,
            String defaultDBName,
            SchemaPlus rootSchema,
            RelNode relNode) {
        final Table table = new SchemaUtils.RelTable(relNode);
        writeLock(() -> SchemaUtils.addTable(fullName, defaultDBName, rootSchema, table));
    }

    /**
     * materialized query.
     *
     * @param relNode query node
     * @return opt node
     */
    public Pair<RelNode, List<RelOptMaterialization>> materializedViewOpt(RelNode relNode) {
        final Set<String> tables = TableRelShuttleImpl.tables(relNode);
        final HepPlanner materializedHepPlanner = createMaterializedHepPlanner();
        List<RelOptMaterialization> views =
                addMaterializeView2Planner(tables, materializedHepPlanner);
        RelNode optRelNode = optimize(relNode, materializedHepPlanner);
        return Pair.of(optRelNode, views);
    }

    /**
     * add RelOptMaterialization to materializedHepPlanner.
     *
     * @param tables tables
     * @param materializedHepPlanner materializedHepPlanner
     */
    private List<RelOptMaterialization> addMaterializeView2Planner(
            Set<String> tables, HepPlanner materializedHepPlanner) {
        List<RelOptMaterialization> views =
                readLock(
                        () -> {
                            List<RelOptMaterialization> tmp = new ArrayList<>();
                            tables.forEach(
                                    table -> {
                                        final List<RelOptMaterialization> viewList =
                                                materializationMap.get(table);
                                        if (viewList != null) {
                                            tmp.addAll(viewList);
                                        }
                                    });
                            return tmp;
                        });
        views.forEach(materializedHepPlanner::addMaterialization);
        return views;
    }

    /**
     * add function.
     *
     * @param name name
     * @param function function
     * @return CalciteContext
     */
    public CalciteContext addFunction(String name, Function function) {
        writeLock(
                () -> {
                    rootSchema.add(name, function);
                    return VOID;
                });
        return this;
    }

    /**
     * sqlNode to relNode.
     *
     * @param sqlNode sqlNode
     * @return relNode
     */
    public RelNode sqlNode2RelNode(SqlNode sqlNode) {
        final RelOptPlanner planner = defaultRelOptPlanner();
        final SqlToRelConverter converter = createSqlToRelConverter(planner);
        final RelRoot viewQueryRoot = converter.convertQuery(sqlNode, true, true);
        return optimize(viewQueryRoot.rel, planner);
    }

    /**
     * optimize relNode.
     *
     * @param relNode relNode
     * @param planner planner
     * @return relNode
     */
    private RelNode optimize(RelNode relNode, RelOptPlanner planner) {
        return writeLock(
                () -> {
                    planner.setRoot(relNode);
                    return planner.findBestExp();
                });
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
        return sqlNode2RelNode(SqlNodeUtils.toSqlNode(sql, queryConfig, sqlShuttles));
    }

    /**
     * relNode toSqlNode.
     *
     * @param relNode relNode
     * @param sqlDialect sqlDialect
     * @return SqlNode
     */
    public SqlNode relNode2SqlNode(RelNode relNode, SqlDialect sqlDialect) {
        final RelToSqlConverter relToSqlConverter =
                new JdbcImplementor(sqlDialect, new UTF16JavaTypeFactoryImp());
        return relToSqlConverter.visitRoot(relNode).asQueryOrValues();
    }

    /**
     * writeLock Function.
     *
     * @param functionHandler functionHandler
     */
    protected final <T> T writeLock(LockUtils.FunctionHandler<T> functionHandler) {
        return lock(functionHandler, lock.writeLock());
    }

    /**
     * readLock Function.
     *
     * @param functionHandler functionHandler
     */
    protected final <T> T readLock(LockUtils.FunctionHandler<T> functionHandler) {
        return lock(functionHandler, lock.readLock());
    }
}
