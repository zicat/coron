package io.agora.cruise.analyzer;

import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlTextIterable;
import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.util.Tuple2;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** PrestoContext. */
public class FileContext extends CalciteContext {

    private static final String DEFAULT_DDL_SOURCE_FILE = "ddl.txt";
    private static final Logger LOG = LoggerFactory.getLogger(FileContext.class);

    public FileContext(String database) {
        this(database, defaultDDLSqlIterator());
    }

    public FileContext(String database, SqlIterator ddlIterator) {
        super(database);
        initSchema(ddlIterator);
    }

    /**
     * default ddl sql.
     *
     * @return SqlIterator
     */
    private static SqlIterator defaultDDLSqlIterator() {
        return new SqlTextIterable(DEFAULT_DDL_SOURCE_FILE, StandardCharsets.UTF_8).sqlIterator();
    }

    /**
     * check materializedViewOpt whether success.
     *
     * @param relNode query node
     * @return match view set and match result RelNode in tuple2
     */
    public Tuple2<Set<String>, RelNode> tryMaterialized(RelNode relNode) {
        Tuple2<RelNode, List<RelOptMaterialization>> tuple = materializedViewOpt(relNode);
        final RelNode optRelNode = tuple.f0;
        final List<String> views =
                tuple.f1.stream()
                        .map(v -> String.join(".", v.qualifiedTableName))
                        .collect(Collectors.toList());
        final Set<String> opRelNode1Tables = TableRelShuttleImpl.tables(optRelNode);
        final Set<String> matchedView = new HashSet<>();
        for (String opRelNode1Table : opRelNode1Tables) {
            if (views.contains(opRelNode1Table)) {
                matchedView.add(opRelNode1Table);
            }
        }
        return Tuple2.of(matchedView, optRelNode);
    }

    /** init schema. */
    private void initSchema(SqlIterator it) {
        try {
            while (it.hasNext()) {
                addTable(it.next());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * parser ddl query.
     *
     * @param ddl ddl
     */
    private void addTable(String ddl) {
        if ((ddl = ddl.trim()).isEmpty()) {
            return;
        }
        String[] split = ddl.split("\\s+");
        String table = split[2];
        String[] tableSplit = table.split("\\.");
        String newTable;
        if (tableSplit.length < 2 || tableSplit.length > 3) {
            LOG.warn("add table fail, ddl:{}", ddl);
            return;
        } else if (tableSplit.length == 2) {
            newTable = tableSplit[0] + ".\"" + tableSplit[1] + "\"";
        } else {
            newTable = tableSplit[1] + ".\"" + tableSplit[2] + "\"";
        }
        ddl = ddl.replace(table, newTable);
        addTables(ddl);
    }
}
