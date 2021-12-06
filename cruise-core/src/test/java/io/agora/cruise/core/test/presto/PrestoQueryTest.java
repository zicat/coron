package io.agora.cruise.core.test.presto;

import com.csvreader.CsvReader;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.test.NodeRelTest;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findSubNode;

/** PrestoQueryTest. */
public class PrestoQueryTest {

    private static final NodeRelTest nodeRelTest;

    static {
        try {
            nodeRelTest = new NodeRelTest();
            LineIterator it =
                    IOUtils.lineIterator(
                            Objects.requireNonNull(
                                    Thread.currentThread()
                                            .getContextClassLoader()
                                            .getResourceAsStream("presto_query/ddl.txt")),
                            StandardCharsets.UTF_8);
            while (it.hasNext()) {
                String ddl = it.next();
                String[] split = ddl.split("\\s+");
                String table = split[2];
                String[] tableSplit = table.split("\\.");
                String newTable = null;
                if (tableSplit.length < 2 || tableSplit.length > 3) {
                    continue;
                } else if (tableSplit.length == 2) {
                    newTable = tableSplit[0] + ".\"" + tableSplit[1] + "\"";
                } else {
                    newTable = tableSplit[1] + ".\"" + tableSplit[2] + "\"";
                }
                ddl = ddl.replace(table, newTable);
                nodeRelTest.addTables(ddl);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testJoin() throws SqlParseException {

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 =
                nodeRelTest.createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 =
                nodeRelTest.createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNode<RelNode> resultNode =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        nodeRelTest.assertResultNode(expectSql, resultNode);
    }

    @Test
    public void test() throws IOException, SqlParseException {
        Set<String> distinctSql = new HashSet<>();
        csvReaderHandler(csvReader -> distinctSql.add(csvReader.get(0)));
        List<String> querySql = new ArrayList<>(distinctSql);
        querySql =
                querySql.stream()
                        .filter(v -> !v.startsWith("explain"))
                        .filter(v -> !v.contains("system.runtime"))
                        .collect(Collectors.toList());
        for (int i = 0; i < querySql.size() - 1; i++) {
            for (int j = i + 1; j < querySql.size(); j++) {
                String fromSql = querySql.get(i);
                String toSql = querySql.get(j);
                final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(fromSql);
                final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(toSql);
                final RelNode relNode1 =
                        nodeRelTest
                                .createSqlToRelConverter()
                                .convertQuery(sqlNode1, true, true)
                                .rel;
                final RelNode relNode2 =
                        nodeRelTest
                                .createSqlToRelConverter()
                                .convertQuery(sqlNode2, true, true)
                                .rel;

                ResultNode<RelNode> resultNode =
                        findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));

                if (!resultNode.isEmpty() && nodeRelTest.toSql(resultNode).contains("GROUP BY")) {
                    System.out.println("==================== similar sql:");
                    System.out.println(nodeRelTest.toSql(resultNode));
                }
            }
        }
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
                                        .getResourceAsStream("presto_query/query.log")),
                        StandardCharsets.UTF_8);
        csvReaderHandle(reader, handler);
    }

    final String sql1 =
            "SELECT CASE TYPEOF(t1.date) WHEN 'integer' THEN CAST(DATE_PARSE(CAST(t1.date AS VARCHAR), '%Y%m%d') AS TIMESTAMP) ELSE CAST(CAST(t1.date AS VARCHAR) AS TIMESTAMP) END AS date,\n"
                    + "  app_type,\n"
                    + "  SUM(t1.audio) AS audio,\n"
                    + "  SUM(t1.video) AS video,\n"
                    + "  SUM(t1.total_usage) AS total_usage\n"
                    + "FROM (SELECT\n"
                    + "    /*+ USE_INDEX(agora_vid_usage_cube_di_date_group_mark_index) */\n"
                    + "    date,\n"
                    + "      company_id AS cid,\n"
                    + "      vid,\n"
                    + "      ver,\n"
                    + "        CASE WHEN ver IN ('2.0.8.2', '2.2.0.20', '2.2.0.27', '2.2.0.27.4', '2.2.0.27.5', '2.2.0.2701', '2.2.0.2703', '2.2.0.29', '2.2.0.60', '2.2.0.61', '2.2.0.62', '2.2.3.20', '2.2.3.201', '2.2.3.24', '2.3.3.110', '2.4.1.20', '2.4.1.42', '2.4.1.43', '2.9.1.42', '2.9.1.43', '2.9.1.44', '2.9.1.45', '2.9.1.46', 'AMG_1.0', 'AMG_1.1', 'AMG_2.0', 'AMG_2.1') AND CAST(app_type AS VARCHAR) = 'UNKNOWN' THEN 'unity' ELSE app_type END AS app_type,\n"
                    + "      ROUND(SUM(audio), 0) AS audio,\n"
                    + "      ROUND(SUM(video), 0) AS video,\n"
                    + "      ROUND(SUM(audio + video), 0) AS total_usage\n"
                    + "    FROM report_datahub.agora_vid_usage_cube_di\n"
                    + "    WHERE date_parse(cast( \"agora_vid_usage_cube_di\".\"date\" as VARCHAR), '%Y%m%d') BETWEEN TIMESTAMP '2021-11-24 00:00:00' AND TIMESTAMP '2021-11-30 00:00:00' AND CAST(group_mark AS VARCHAR) = '(product_line,product_type,domain,country,vid,os,ver,app_type)'\n"
                    + "    GROUP BY date,\n"
                    + "      company_id,\n"
                    + "      vid,\n"
                    + "      ver,\n"
                    + "        CASE WHEN ver IN ('2.0.8.2', '2.2.0.20', '2.2.0.27', '2.2.0.27.4', '2.2.0.27.5', '2.2.0.2701', '2.2.0.2703', '2.2.0.29', '2.2.0.60', '2.2.0.61', '2.2.0.62', '2.2.3.20', '2.2.3.201', '2.2.3.24', '2.3.3.110', '2.4.1.20', '2.4.1.42', '2.4.1.43', '2.9.1.42', '2.9.1.43', '2.9.1.44', '2.9.1.45', '2.9.1.46', 'AMG_1.0', 'AMG_1.1', 'AMG_2.0', 'AMG_2.1') AND CAST(app_type AS VARCHAR) = 'UNKNOWN' THEN 'unity' ELSE app_type END) AS t1\n"
                    + "  INNER JOIN (SELECT vid,\n"
                    + "      companyname,\n"
                    + "      projectname,\n"
                    + "      country\n"
                    + "    FROM report_datahub.new_vendor_dimension\n"
                    + "    WHERE 1 = 1) AS t2 ON CAST(t1.vid AS VARCHAR) = CAST(t2.vid AS VARCHAR)\n"
                    + "WHERE 1 = 1 AND CAST(app_type AS VARCHAR) = 'react_native'\n"
                    + "GROUP BY date,\n"
                    + "  app_type";
    final String sql2 =
            "SELECT CASE TYPEOF(t1.date) WHEN 'integer' THEN CAST(DATE_PARSE(CAST(t1.date AS VARCHAR), '%Y%m%d') AS TIMESTAMP) ELSE CAST(CAST(t1.date AS VARCHAR) AS TIMESTAMP) END AS date,\n"
                    + "  app_type,\n"
                    + "  SUM(t1.audio) AS audio,\n"
                    + "  SUM(t1.video) AS video,\n"
                    + "  SUM(t1.total_usage) AS total_usage\n"
                    + "FROM (SELECT\n"
                    + "    /*+ USE_INDEX(agora_vid_usage_cube_di_date_group_mark_index) */\n"
                    + "    date,\n"
                    + "      company_id AS cid,\n"
                    + "      vid,\n"
                    + "      ver,\n"
                    + "        CASE WHEN ver IN ('2.0.8.2', '2.2.0.20', '2.2.0.27', '2.2.0.27.4', '2.2.0.27.5', '2.2.0.2701', '2.2.0.2703', '2.2.0.29', '2.2.0.60', '2.2.0.61', '2.2.0.62', '2.2.3.20', '2.2.3.201', '2.2.3.24', '2.3.3.110', '2.4.1.20', '2.4.1.42', '2.4.1.43', '2.9.1.42', '2.9.1.43', '2.9.1.44', '2.9.1.45', '2.9.1.46', 'AMG_1.0', 'AMG_1.1', 'AMG_2.0', 'AMG_2.1') AND CAST(app_type AS VARCHAR) = 'UNKNOWN' THEN 'unity' ELSE app_type END AS app_type,\n"
                    + "      ROUND(SUM(audio), 0) AS audio,\n"
                    + "      ROUND(SUM(video), 0) AS video,\n"
                    + "      ROUND(SUM(audio + video), 0) AS total_usage\n"
                    + "    FROM report_datahub.agora_vid_usage_cube_di\n"
                    + "    WHERE date_parse(cast( \"agora_vid_usage_cube_di\".\"date\" as VARCHAR), '%Y%m%d') BETWEEN TIMESTAMP '2021-11-24 00:00:00' AND TIMESTAMP '2021-11-30 00:00:00' AND CAST(group_mark AS VARCHAR) = '(product_line,product_type,domain,country,vid,os,ver,app_type)'\n"
                    + "    GROUP BY date,\n"
                    + "      company_id,\n"
                    + "      vid,\n"
                    + "      ver,\n"
                    + "        CASE WHEN ver IN ('2.0.8.2', '2.2.0.20', '2.2.0.27', '2.2.0.27.4', '2.2.0.27.5', '2.2.0.2701', '2.2.0.2703', '2.2.0.29', '2.2.0.60', '2.2.0.61', '2.2.0.62', '2.2.3.20', '2.2.3.201', '2.2.3.24', '2.3.3.110', '2.4.1.20', '2.4.1.42', '2.4.1.43', '2.9.1.42', '2.9.1.43', '2.9.1.44', '2.9.1.45', '2.9.1.46', 'AMG_1.0', 'AMG_1.1', 'AMG_2.0', 'AMG_2.1') AND CAST(app_type AS VARCHAR) = 'UNKNOWN' THEN 'unity' ELSE app_type END) AS t1\n"
                    + "  INNER JOIN (SELECT vid,\n"
                    + "      companyname,\n"
                    + "      projectname,\n"
                    + "      country\n"
                    + "    FROM report_datahub.new_vendor_dimension\n"
                    + "    WHERE 1 = 1) AS t2 ON CAST(t1.vid AS VARCHAR) = CAST(t2.vid AS VARCHAR)\n"
                    + "WHERE 1 = 1 AND CAST(app_type AS VARCHAR) = 'unity'\n"
                    + "GROUP BY date,\n"
                    + "  app_type";

    final String expectSql =
            "SELECT app_type, audio, CASE WHEN TYPEOF(date) = 'integer' THEN CAST(DATE_PARSE(CAST(date AS VARCHAR), '%Y%m%d') AS TIMESTAMP(0)) ELSE CAST(CAST(date AS VARCHAR) AS TIMESTAMP(0)) END date, total_usage, video\n"
                    + "FROM (SELECT t2.app_type, t2.audio, t2.cid, t4.companyname, t4.country, t2.date, t4.projectname, t2.total_usage, t2.ver, t2.vid, t4.vid vid0, t2.video\n"
                    + "FROM (SELECT CASE WHEN ver IN ('2.0.8.2', '2.2.0.20', '2.2.0.27', '2.2.0.27.4', '2.2.0.27.5', '2.2.0.2701', '2.2.0.2703', '2.2.0.29', '2.2.0.60', '2.2.0.61', '2.2.0.62', '2.2.3.20', '2.2.3.201', '2.2.3.24', '2.3.3.110', '2.4.1.20', '2.4.1.42', '2.4.1.43', '2.9.1.42', '2.9.1.43', '2.9.1.44', '2.9.1.45', '2.9.1.46', 'AMG_1.0', 'AMG_1.1', 'AMG_2.0', 'AMG_2.1') AND app_type = 'UNKNOWN' THEN 'unity' ELSE app_type END app_type, ROUND(SUM(audio), 0) audio, company_id cid, date, ROUND(SUM(audio + video), 0) total_usage, ver, vid, ROUND(SUM(video), 0) video\n"
                    + "FROM report_datahub.agora_vid_usage_cube_di\n"
                    + "WHERE date_parse(CAST(date AS VARCHAR), '%Y%m%d') >= TIMESTAMP '2021-11-24 00:00:00' AND date_parse(CAST(date AS VARCHAR), '%Y%m%d') <= TIMESTAMP '2021-11-30 00:00:00' AND group_mark = '(product_line,product_type,domain,country,vid,os,ver,app_type)'\n"
                    + "GROUP BY CASE WHEN ver IN ('2.0.8.2', '2.2.0.20', '2.2.0.27', '2.2.0.27.4', '2.2.0.27.5', '2.2.0.2701', '2.2.0.2703', '2.2.0.29', '2.2.0.60', '2.2.0.61', '2.2.0.62', '2.2.3.20', '2.2.3.201', '2.2.3.24', '2.3.3.110', '2.4.1.20', '2.4.1.42', '2.4.1.43', '2.9.1.42', '2.9.1.43', '2.9.1.44', '2.9.1.45', '2.9.1.46', 'AMG_1.0', 'AMG_1.1', 'AMG_2.0', 'AMG_2.1') AND app_type = 'UNKNOWN' THEN 'unity' ELSE app_type END, company_id, date, ver, vid) t2\n"
                    + "INNER JOIN (SELECT companyname, country, projectname, vid, CAST(vid AS VARCHAR) vid0\n"
                    + "FROM report_datahub.new_vendor_dimension\n"
                    + "WHERE 1 = 1) t4 ON t2.cid = t4.vid0) t5\n"
                    + "WHERE 1 = 1 AND t5.app_type = 'unity' OR 1 = 1 AND t5.app_type = 'react_native'";
}
