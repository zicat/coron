package io.agora.cruise.presto;

import com.csvreader.CsvReader;
import io.agora.cruise.parser.CalciteContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.TableRelShuttleImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/** PrestoContext. */
public class PrestoContext extends CalciteContext {

    public PrestoContext() {
        super("report_datahub");
        init();
    }

    public boolean canMaterialized(RelNode relNode, Set<String> viewNames) {
        RelNode optRelNode1 = materializedViewOpt(relNode);
        Set<String> opRelNode1Tables = TableRelShuttleImpl.tables(optRelNode1);
        for (String opRelNode1Table : opRelNode1Tables) {
            if (viewNames.contains(opRelNode1Table)) {
                return true;
            }
        }
        return false;
    }

    public static List<String> querySqlList() throws IOException {
        Set<String> distinctSql = new HashSet<>();
        csvReaderHandler(csvReader -> distinctSql.add(csvReader.get(0)));
        List<String> querySql = new ArrayList<>(distinctSql);
        querySql =
                querySql.stream()
                        .filter(v -> !v.startsWith("explain"))
                        .filter(v -> !v.contains("system.runtime"))
                        .filter(v -> !v.toUpperCase().startsWith("SHOW "))
                        .collect(Collectors.toList());
        return querySql;
    }

    public static BufferedWriter createWriter(String fileName) throws IOException {
        File file = new File(fileName);
        if (file.exists() && !file.delete()) {
            throw new IOException("delete fail" + fileName);
        }
        if (!file.createNewFile()) {
            throw new IOException("create fail " + fileName);
        }
        return new BufferedWriter(new FileWriter(file));
    }

    /** Handler. */
    public interface Handler {

        void handle(CsvReader csvReader) throws IOException;
    }

    public static void csvReaderHandle(Reader reader, Handler handler) throws IOException {
        CsvReader csvReader = new CsvReader(reader);
        csvReader.readHeaders();
        while (csvReader.readRecord()) {
            handler.handle(csvReader);
        }
    }

    public static void csvReaderHandler(Handler handler) throws IOException {
        Reader reader =
                new InputStreamReader(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("query2.log")),
                        StandardCharsets.UTF_8);
        csvReaderHandle(reader, handler);
    }

    private void init() {
        try {
            LineIterator it =
                    IOUtils.lineIterator(
                            Objects.requireNonNull(
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResourceAsStream("ddl.txt")),
                            StandardCharsets.UTF_8);
            while (it.hasNext()) {
                String ddl = it.next();
                String[] split = ddl.split("\\s+");
                String table = split[2];
                String[] tableSplit = table.split("\\.");
                String newTable;
                if (tableSplit.length < 2 || tableSplit.length > 3) {
                    continue;
                } else if (tableSplit.length == 2) {
                    newTable = tableSplit[0] + ".\"" + tableSplit[1] + "\"";
                } else {
                    newTable = tableSplit[1] + ".\"" + tableSplit[2] + "\"";
                }
                ddl = ddl.replace(table, newTable);
                addTables(ddl);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
