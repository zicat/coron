package io.agora.cruise.parser;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import javax.annotation.Nullable;
import java.util.List;

import static io.agora.cruise.parser.SqlNodeTool.DEFAULT_QUERY_PARSER_CONFIG;

/** SqlToRelConverterTool. */
public class SqlToRelConverterTool {

    /**
     * create sql to rel converter.
     *
     * @param planner planner
     * @param factory factory
     * @param calciteCatalogReader calcite catalog reader
     * @param sqlOperatorTable sqlOperatorTable
     * @param schemaPlus schema plus
     * @param parserConfig parser config
     * @return SqlToRelConverter
     */
    public static SqlToRelConverter createSqlToRelConverter(
            RelOptPlanner planner,
            SqlTypeFactoryImpl factory,
            CalciteCatalogReader calciteCatalogReader,
            SqlOperatorTable sqlOperatorTable,
            SchemaPlus schemaPlus,
            SqlValidator validator,
            @Nullable SqlParser.Config parserConfig) {

        final FrameworkConfig frameworkConfig =
                Frameworks.newConfigBuilder()
                        .parserConfig(
                                parserConfig == null ? DEFAULT_QUERY_PARSER_CONFIG : parserConfig)
                        .defaultSchema(schemaPlus)
                        .operatorTable(sqlOperatorTable)
                        .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                        .build();

        final RexBuilder rexBuilder = new RexBuilder(factory);
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        final SqlToRelConverter.Config config =
                SqlToRelConverter.config()
                        .withTrimUnusedFields(true)
                        .withExplain(false)
                        .withInSubQueryThreshold(Integer.MAX_VALUE);
        return new SqlToRelConverter(
                new ViewExpanderImpl(),
                validator,
                calciteCatalogReader,
                cluster,
                frameworkConfig.getConvertletTable(),
                config);
    }

    /** ViewExpanderImpl. */
    public static class ViewExpanderImpl implements RelOptTable.ViewExpander {
        public ViewExpanderImpl() {}

        @Override
        public RelRoot expandView(
                RelDataType rowType,
                String queryString,
                List<String> schemaPath,
                @Nullable List<String> viewPath) {
            return null;
        }
    }
}
