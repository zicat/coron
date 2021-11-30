package io.agora.cruise.parser.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorWithHints;

/** ClickHouseSqlValidatorImpl. */
public class CruiseSqlValidatorImpl extends SqlValidatorImpl {

    protected SqlTypeFactoryImpl sqlTypeFactory;

    @Override
    public RelDataType getValidatedNodeType(SqlNode node) {
        if (!(node instanceof SqlIdentifier)) {
            return super.getValidatedNodeType(node);
        }
        SqlIdentifier sqlIdentifier = (SqlIdentifier) node;
        if (sqlIdentifier.names.size() != 1) {
            return super.getValidatedNodeType(node);
        }

        switch (sqlIdentifier.getSimple()) {
            case "STRING":
                return sqlTypeFactory.createSqlType(SqlTypeName.VARCHAR);
            case "INT64":
                return sqlTypeFactory.createSqlType(SqlTypeName.BIGINT);
            case "INT32":
                return sqlTypeFactory.createSqlType(SqlTypeName.INTEGER);
            default:
                return super.getValidatedNodeType(node);
        }
    }

    /**
     * Creates a validator.
     *
     * @param opTab Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory Type factory
     * @param config Config
     */
    protected CruiseSqlValidatorImpl(
            SqlTypeFactoryImpl sqlTypeFactory,
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config) {
        super(opTab, catalogReader, typeFactory, config);
        this.sqlTypeFactory = sqlTypeFactory;
    }

    @Override
    public boolean isAggregate(SqlNode selectNode) {
        return false;
    }

    public static SqlValidatorWithHints newValidator(
            SqlTypeFactoryImpl sqlTypeFactory,
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config) {
        return new CruiseSqlValidatorImpl(
                sqlTypeFactory, opTab, catalogReader, typeFactory, config);
    }
}
