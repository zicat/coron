package io.agora.cruise.analyzer;

import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlTextIterable;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.calcite.sql.parser.SqlParseException;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** PrestoContext. */
public class FileContext extends CalciteContext {

    private static final String DEFAULT_DDL_SOURCE_FILE = "ddl.txt";

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
     * @param viewNames view name
     * @return match view set
     */
    public Set<String> canMaterialized(RelNode relNode, Set<String> viewNames) {
        return canMaterializedWithRelNode(relNode, viewNames).f0;
    }

    /**
     * check materializedViewOpt whether success.
     *
     * @param relNode query node
     * @param viewNames view name
     * @return match view set
     */
    public Tuple2<Set<String>, RelNode> canMaterializedWithRelNode(
            RelNode relNode, Set<String> viewNames) {
        RelNode optRelNode1 = materializedViewOpt(relNode);
        Set<String> opRelNode1Tables = TableRelShuttleImpl.tables(optRelNode1);
        Set<String> matchedView = new HashSet<>();
        for (String opRelNode1Table : opRelNode1Tables) {
            if (viewNames.contains(opRelNode1Table)) {
                matchedView.add(opRelNode1Table);
            }
        }
        return Tuple2.of(matchedView, optRelNode1);
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
     * @throws SqlParseException SqlParseException
     */
    private void addTable(String ddl) throws SqlParseException {
        ddl = ddl.trim();
        if (ddl.isEmpty()) {
            return;
        }
        String[] split = ddl.split("\\s+");
        String table = split[2];
        String[] tableSplit = table.split("\\.");
        String newTable;
        if (tableSplit.length < 2 || tableSplit.length > 3) {
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
