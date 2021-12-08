package io.agora.cruise.core.test.presto;

import com.csvreader.CsvReader;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.test.NodeRelTest;
import io.agora.cruise.parser.SqlNodeTool;
import io.agora.cruise.parser.sql.presto.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static io.agora.cruise.core.NodeUtils.createNodeRelRoot;
import static io.agora.cruise.core.NodeUtils.findSubNode;

/** PrestoQueryTest. */
public class PrestoQueryTest {

    //    @Test
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

    public static void main(String[] args) throws Exception {
        Set<String> distinctSql = new HashSet<>();
        csvReaderHandler(csvReader -> distinctSql.add(csvReader.get(0)));
        List<String> querySql = new ArrayList<>(distinctSql);
        querySql =
                querySql.stream()
                        .filter(v -> !v.startsWith("explain"))
                        .filter(v -> !v.contains("system.runtime"))
                        .filter(v -> !v.toUpperCase().startsWith("SHOW "))
                        .collect(Collectors.toList());
        File file = new File(System.getProperty("user.home") + "/Desktop/public-sql.txt");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        BufferedWriter bw = new BufferedWriter(new FileWriter(file));

        File file2 = new File(System.getProperty("user.home") + "/Desktop/source-sql.txt");
        if (file2.exists()) {
            file2.delete();
        }
        file2.createNewFile();
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(file2));
        Map<String, Integer> result = new HashMap<>();
        for (int i = 0; i < querySql.size() - 1; i++) {
            for (int j = i + 1; j < querySql.size(); j++) {
                String fromSql = querySql.get(i);
                String toSql = querySql.get(j);
                try {
                    final SqlNode sqlNode1 =
                            SqlNodeTool.toQuerySqlNode(fromSql, new Int2BooleanConditionShuttle());
                    final SqlNode sqlNode2 =
                            SqlNodeTool.toQuerySqlNode(toSql, new Int2BooleanConditionShuttle());
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
                    if (resultNode.isEmpty()) {
                        continue;
                    }

                    String resultSql = nodeRelTest.toSql(resultNode);
                    if (!resultSql.contains("GROUP BY")) {
                        continue;
                    }
                    Integer value = result.get(resultSql);
                    if (value == null) {
                        value = result.size();
                        result.put(resultSql, value);
                        bw.write(
                                "======================similar sql "
                                        + value
                                        + "==================");
                        bw.newLine();
                        bw.write(nodeRelTest.toSql(resultNode));
                        bw.newLine();
                        bw.flush();
                    }
                    bw2.write(
                            "=========================from sql mapping to ="
                                    + value
                                    + "========================");
                    bw2.newLine();
                    bw2.write(fromSql);
                    bw2.newLine();
                    bw2.write(
                            "=========================to sql mapping to ="
                                    + value
                                    + "========================");
                    bw2.newLine();
                    bw2.write(toSql);
                    bw2.newLine();
                    bw2.flush();

                } catch (Throwable e) {
                    if (e.toString().contains("Object 'media' not found")
                            || e.toString().contains("Object 'queries' not found")
                            || e.toString().contains("Object 'information_schema' not found")) {
                        continue;
                    }
                    System.out.println("======================");
                    System.out.println(fromSql);
                    System.out.println("======================");
                    System.out.println(toSql);
                    throw e;
                }
            }
        }
        bw.close();
        bw2.close();
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
                                        .getResourceAsStream("presto_query/query2.log")),
                        StandardCharsets.UTF_8);
        csvReaderHandle(reader, handler);
    }

    private static final NodeRelTest nodeRelTest;

    static {
        try {
            nodeRelTest = new NodeRelTest("report_datahub");
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
                String newTable;
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

    final String sql1 =
            "SELECT SUM(\"自定义 SQL 查询\".\"audience_low_latency_last1\") AS \"TEMP(Calculation_230316907702542336)(2007846651)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_low_latency\") AS \"TEMP(Calculation_230316907702542336)(2627877912)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"total\") AS \"TEMP(Calculation_5203557528859017219)(2253843611)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"total_last1\") AS \"TEMP(Calculation_5203557528859017219)(3076278274)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audio_last1\") AS \"TEMP(Calculation_5203557528863870981)(2207581090)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audio\") AS \"TEMP(Calculation_5203557528863870981)(4234702922)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"video_last1\") AS \"TEMP(Calculation_5203557528864096263)(1192334319)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"video\") AS \"TEMP(Calculation_5203557528864096263)(3647352276)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience\") AS \"TEMP(Calculation_534802460955746306)(1306364424)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_last1\") AS \"TEMP(Calculation_534802460955746306)(2505681848)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio\") AS \"TEMP(Calculation_534802460956037124)(1868547182)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio_last1\") AS \"TEMP(Calculation_534802460956037124)(2218970377)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_last1\") AS \"TEMP(Calculation_534802460956319750)(188915252)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video\") AS \"TEMP(Calculation_534802460956319750)(2182583436)(0)\",\n"
                    + "  MAX(\"自定义 SQL 查询\".\"stat_date\") AS \"TEMP(attr_stat_date_nk)(2102638629)(0)\",\n"
                    + "  MIN(\"自定义 SQL 查询\".\"stat_date\") AS \"TEMP(attr_stat_date_nk)(2790680713)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio_low_latency_last1\") AS \"TEMP(audience_audio_last1_rate (复制)_230316907704033282)(67\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio_low_latency\") AS \"TEMP(audience_audio_last1_rate (复制)_230316907704033282)(83\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_audio_last1\") AS \"TEMP(audience_audio_last1_rate (复制)_534802460956618761)(24\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_audio\") AS \"TEMP(audience_audio_last1_rate (复制)_534802460956618761)(36\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_last1\") AS \"TEMP(audience_last1_rate (复制)_534802460956647435)(15035501\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host\") AS \"TEMP(audience_last1_rate (复制)_534802460956647435)(24867219\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_low_latency_last1\") AS \"TEMP(audience_low_latency_last1_rate (复制)_2303169077218304\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_low_latency\") AS \"TEMP(audience_low_latency_last1_rate (复制)_2303169077218301\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_video\") AS \"TEMP(audience_video_last1_rate (复制)_534802460956696589)(26\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_video_last1\") AS \"TEMP(audience_video_last1_rate (复制)_534802460956696589)(42\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_low_latency\") AS \"sum_audience_video_low_latency_ok\"\n"
                    + "FROM (SELECT date,\n"
                    + "      SUBSTR(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) AS date_range_1,\n"
                    + "      SUBSTR(CAST(end_date AS VARCHAR), 1, 10) AS date_range_2,\n"
                    + "      stat_period,\n"
                    + "        CASE WHEN CAST(stat_period AS VARCHAR) = 'day' THEN substr(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) ELSE concat(CAST(date_format(CAST(start_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR), '~', CAST(date_format(CAST(end_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR)) END AS stat_date,\n"
                    + "      area,\n"
                    + "      vid,\n"
                    + "      project,\n"
                    + "      company_id AS cid,\n"
                    + "      company_name AS company,\n"
                    + "      customer_level AS level,\n"
                    + "      vid_internal_industry_en AS industry,\n"
                    + "      vid_use_case_cn,\n"
                    + "      csdc_owner,\n"
                    + "      os,\n"
                    + "      ver,\n"
                    + "      ver_type,\n"
                    + "      product_level1,\n"
                    + "      product_level2,\n"
                    + "      product_level3,\n"
                    + "      total,\n"
                    + "      audio,\n"
                    + "      video,\n"
                    + "      video_sd,\n"
                    + "      video_hd,\n"
                    + "      video_hdp,\n"
                    + "        host_audio + host_video AS host,\n"
                    + "      host_audio,\n"
                    + "      host_video,\n"
                    + "      host_sd,\n"
                    + "      host_hd,\n"
                    + "      host_hdp,\n"
                    + "        audience_audio + audience_video AS audience,\n"
                    + "        audience_audio_low_latency + audience_video_low_latency AS audience_low_latency,\n"
                    + "      audience_audio,\n"
                    + "      audience_audio_low_latency,\n"
                    + "      audience_video,\n"
                    + "      audience_video_low_latency,\n"
                    + "      audience_sd,\n"
                    + "      audience_sd_low_latency,\n"
                    + "      audience_hd,\n"
                    + "      audience_hd_low_latency,\n"
                    + "      audience_hdp,\n"
                    + "      audience_hdp_low_latency,\n"
                    + "      total_last1,\n"
                    + "      audio_last1,\n"
                    + "      video_last1,\n"
                    + "      video_sd_last1,\n"
                    + "      video_hd_last1,\n"
                    + "      video_hdp_last1,\n"
                    + "        host_audio_last1 + host_video_last1 AS host_last1,\n"
                    + "      host_audio_last1,\n"
                    + "      host_video_last1,\n"
                    + "        audience_audio_last1 + audience_video_last1 AS audience_last1,\n"
                    + "        audience_audio_low_latency_last1 + audience_video_low_latency_last1 AS audience_low_latency_last1,\n"
                    + "      audience_audio_last1,\n"
                    + "      audience_audio_low_latency_last1,\n"
                    + "      audience_video_last1,\n"
                    + "      audience_video_low_latency_last1,\n"
                    + "      total_last7,\n"
                    + "      audio_last7,\n"
                    + "      video_last7,\n"
                    + "      video_sd_last7,\n"
                    + "      video_hd_last7,\n"
                    + "      video_hdp_last7,\n"
                    + "        host_audio_last7 + host_video_last7 AS host_last7,\n"
                    + "      host_audio_last7,\n"
                    + "      host_video_last7,\n"
                    + "        audience_audio_last7 + audience_video_last7 AS audience_last7,\n"
                    + "        audience_audio_low_latency_last7 + audience_video_low_latency_last7 AS audience_low_latency_last7,\n"
                    + "      audience_audio_last7,\n"
                    + "      audience_audio_low_latency_last7,\n"
                    + "      audience_video_last7,\n"
                    + "      audience_video_low_latency_last7\n"
                    + "    FROM report_datahub.vendor_vid_usage_di_1\n"
                    + "    WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-06') AND date_parse(cast( date as VARCHAR), '%Y%m%d') <= DATE('2021-12-06') AND CAST(vid AS VARCHAR) = CAST(428342 AS VARCHAR) AND dim = 1) AS \"自定义 SQL 查询\"\n"
                    + "  CROSS JOIN (SELECT MAX(date_parse(cast( \"自定义 SQL 查询\".\"date\" as VARCHAR), '%Y%m%d')) AS \"__measure__0\"\n"
                    + "    FROM (SELECT date,\n"
                    + "          SUBSTR(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) AS date_range_1,\n"
                    + "          SUBSTR(CAST(end_date AS VARCHAR), 1, 10) AS date_range_2,\n"
                    + "          stat_period,\n"
                    + "            CASE WHEN CAST(stat_period AS VARCHAR) = 'day' THEN substr(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) ELSE concat(CAST(date_format(CAST(start_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR), '~', CAST(date_format(CAST(end_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR)) END AS stat_date,\n"
                    + "          area,\n"
                    + "          vid,\n"
                    + "          project,\n"
                    + "          company_id AS cid,\n"
                    + "          company_name AS company,\n"
                    + "          customer_level AS level,\n"
                    + "          vid_internal_industry_en AS industry,\n"
                    + "          vid_use_case_cn,\n"
                    + "          csdc_owner,\n"
                    + "          os,\n"
                    + "          ver,\n"
                    + "          ver_type,\n"
                    + "          product_level1,\n"
                    + "          product_level2,\n"
                    + "          product_level3,\n"
                    + "          total,\n"
                    + "          audio,\n"
                    + "          video,\n"
                    + "          video_sd,\n"
                    + "          video_hd,\n"
                    + "          video_hdp,\n"
                    + "            host_audio + host_video AS host,\n"
                    + "          host_audio,\n"
                    + "          host_video,\n"
                    + "          host_sd,\n"
                    + "          host_hd,\n"
                    + "          host_hdp,\n"
                    + "            audience_audio + audience_video AS audience,\n"
                    + "            audience_audio_low_latency + audience_video_low_latency AS audience_low_latency,\n"
                    + "          audience_audio,\n"
                    + "          audience_audio_low_latency,\n"
                    + "          audience_video,\n"
                    + "          audience_video_low_latency,\n"
                    + "          audience_sd,\n"
                    + "          audience_sd_low_latency,\n"
                    + "          audience_hd,\n"
                    + "          audience_hd_low_latency,\n"
                    + "          audience_hdp,\n"
                    + "          audience_hdp_low_latency,\n"
                    + "          total_last1,\n"
                    + "          audio_last1,\n"
                    + "          video_last1,\n"
                    + "          video_sd_last1,\n"
                    + "          video_hd_last1,\n"
                    + "          video_hdp_last1,\n"
                    + "            host_audio_last1 + host_video_last1 AS host_last1,\n"
                    + "          host_audio_last1,\n"
                    + "          host_video_last1,\n"
                    + "            audience_audio_last1 + audience_video_last1 AS audience_last1,\n"
                    + "            audience_audio_low_latency_last1 + audience_video_low_latency_last1 AS audience_low_latency_last1,\n"
                    + "          audience_audio_last1,\n"
                    + "          audience_audio_low_latency_last1,\n"
                    + "          audience_video_last1,\n"
                    + "          audience_video_low_latency_last1,\n"
                    + "          total_last7,\n"
                    + "          audio_last7,\n"
                    + "          video_last7,\n"
                    + "          video_sd_last7,\n"
                    + "          video_hd_last7,\n"
                    + "          video_hdp_last7,\n"
                    + "            host_audio_last7 + host_video_last7 AS host_last7,\n"
                    + "          host_audio_last7,\n"
                    + "          host_video_last7,\n"
                    + "            audience_audio_last7 + audience_video_last7 AS audience_last7,\n"
                    + "            audience_audio_low_latency_last7 + audience_video_low_latency_last7 AS audience_low_latency_last7,\n"
                    + "          audience_audio_last7,\n"
                    + "          audience_audio_low_latency_last7,\n"
                    + "          audience_video_last7,\n"
                    + "          audience_video_low_latency_last7\n"
                    + "        FROM report_datahub.vendor_vid_usage_di_1\n"
                    + "        WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-06') AND date_parse(cast( date as VARCHAR), '%Y%m%d') <= DATE('2021-12-06') AND CAST(vid AS VARCHAR) = CAST(428342 AS VARCHAR) AND dim = 1) AS \"自定义 SQL 查询\"\n"
                    + "    HAVING COUNT(1) > 0) AS \"t0\"\n"
                    + "WHERE date_parse(cast( \"自定义 SQL 查询\".\"date\" as VARCHAR), '%Y%m%d') = \"t0\".\"__measure__0\"\n"
                    + "HAVING COUNT(1) > 0";
    final String sql2 =
            "SELECT SUM(\"自定义 SQL 查询\".\"total\") AS \"TEMP(Calculation_5203557528862519300)(2253843611)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"total_last7\") AS \"TEMP(Calculation_5203557528862519300)(431428601)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audio_last7\") AS \"TEMP(Calculation_5203557528863973382)(1745737303)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audio\") AS \"TEMP(Calculation_5203557528863973382)(4234702922)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"video_last7\") AS \"TEMP(Calculation_5203557528864174088)(3553322618)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"video\") AS \"TEMP(Calculation_5203557528864174088)(3647352276)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience\") AS \"TEMP(Calculation_534802460955955203)(1306364424)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_last7\") AS \"TEMP(Calculation_534802460955955203)(2563414513)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio_last7\") AS \"TEMP(Calculation_534802460956184581)(1783975840)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio\") AS \"TEMP(Calculation_534802460956184581)(1868547182)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_last7\") AS \"TEMP(Calculation_534802460956430343)(1897793420)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video\") AS \"TEMP(Calculation_534802460956430343)(2182583436)(0)\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio_low_latency_last7\") AS \"TEMP(audience_audio_last7_rate (复制)_230316907704053763)(29\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_audio_low_latency\") AS \"TEMP(audience_audio_last7_rate (复制)_230316907704053763)(83\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_audio_last7\") AS \"TEMP(audience_audio_last7_rate (复制)_534802460956631050)(15\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_audio\") AS \"TEMP(audience_audio_last7_rate (复制)_534802460956631050)(36\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_last7\") AS \"TEMP(audience_last7_date (复制)_534802460957310992)(24217528\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host\") AS \"TEMP(audience_last7_date (复制)_534802460957310992)(24867219\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_low_latency\") AS \"TEMP(audience_low_latency_last1_rate (复制)_2303169077032468\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_low_latency_last7\") AS \"TEMP(audience_low_latency_last1_rate (复制)_2303169077032461\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_low_latency\") AS \"TEMP(audience_low_latency_last7_rate (复制)_2303169077218181\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"audience_video_low_latency_last7\") AS \"TEMP(audience_low_latency_last7_rate (复制)_2303169077218182\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_video_last7\") AS \"TEMP(audience_video_last7_rate (复制)_534802460956704782)(21\",\n"
                    + "  SUM(\"自定义 SQL 查询\".\"host_video\") AS \"TEMP(audience_video_last7_rate (复制)_534802460956704782)(26\"\n"
                    + "FROM (SELECT date,\n"
                    + "      SUBSTR(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) AS date_range_1,\n"
                    + "      SUBSTR(CAST(end_date AS VARCHAR), 1, 10) AS date_range_2,\n"
                    + "      stat_period,\n"
                    + "        CASE WHEN CAST(stat_period AS VARCHAR) = 'day' THEN substr(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) ELSE concat(CAST(date_format(CAST(start_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR), '~', CAST(date_format(CAST(end_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR)) END AS stat_date,\n"
                    + "      area,\n"
                    + "      vid,\n"
                    + "      project,\n"
                    + "      company_id AS cid,\n"
                    + "      company_name AS company,\n"
                    + "      customer_level AS level,\n"
                    + "      vid_internal_industry_en AS industry,\n"
                    + "      vid_use_case_cn,\n"
                    + "      csdc_owner,\n"
                    + "      os,\n"
                    + "      ver,\n"
                    + "      ver_type,\n"
                    + "      product_level1,\n"
                    + "      product_level2,\n"
                    + "      product_level3,\n"
                    + "      total,\n"
                    + "      audio,\n"
                    + "      video,\n"
                    + "      video_sd,\n"
                    + "      video_hd,\n"
                    + "      video_hdp,\n"
                    + "        host_audio + host_video AS host,\n"
                    + "      host_audio,\n"
                    + "      host_video,\n"
                    + "      host_sd,\n"
                    + "      host_hd,\n"
                    + "      host_hdp,\n"
                    + "        audience_audio + audience_video AS audience,\n"
                    + "        audience_audio_low_latency + audience_video_low_latency AS audience_low_latency,\n"
                    + "      audience_audio,\n"
                    + "      audience_audio_low_latency,\n"
                    + "      audience_video,\n"
                    + "      audience_video_low_latency,\n"
                    + "      audience_sd,\n"
                    + "      audience_sd_low_latency,\n"
                    + "      audience_hd,\n"
                    + "      audience_hd_low_latency,\n"
                    + "      audience_hdp,\n"
                    + "      audience_hdp_low_latency,\n"
                    + "      total_last1,\n"
                    + "      audio_last1,\n"
                    + "      video_last1,\n"
                    + "      video_sd_last1,\n"
                    + "      video_hd_last1,\n"
                    + "      video_hdp_last1,\n"
                    + "        host_audio_last1 + host_video_last1 AS host_last1,\n"
                    + "      host_audio_last1,\n"
                    + "      host_video_last1,\n"
                    + "        audience_audio_last1 + audience_video_last1 AS audience_last1,\n"
                    + "        audience_audio_low_latency_last1 + audience_video_low_latency_last1 AS audience_low_latency_last1,\n"
                    + "      audience_audio_last1,\n"
                    + "      audience_audio_low_latency_last1,\n"
                    + "      audience_video_last1,\n"
                    + "      audience_video_low_latency_last1,\n"
                    + "      total_last7,\n"
                    + "      audio_last7,\n"
                    + "      video_last7,\n"
                    + "      video_sd_last7,\n"
                    + "      video_hd_last7,\n"
                    + "      video_hdp_last7,\n"
                    + "        host_audio_last7 + host_video_last7 AS host_last7,\n"
                    + "      host_audio_last7,\n"
                    + "      host_video_last7,\n"
                    + "        audience_audio_last7 + audience_video_last7 AS audience_last7,\n"
                    + "        audience_audio_low_latency_last7 + audience_video_low_latency_last7 AS audience_low_latency_last7,\n"
                    + "      audience_audio_last7,\n"
                    + "      audience_audio_low_latency_last7,\n"
                    + "      audience_video_last7,\n"
                    + "      audience_video_low_latency_last7\n"
                    + "    FROM report_datahub.vendor_vid_usage_di_1\n"
                    + "    WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-06') AND date_parse(cast( date as VARCHAR), '%Y%m%d') <= DATE('2021-12-06') AND CAST(vid AS VARCHAR) = CAST(428342 AS VARCHAR) AND dim = 1) AS \"自定义 SQL 查询\"\n"
                    + "  CROSS JOIN (SELECT MAX(date_parse(cast( \"自定义 SQL 查询\".\"date\" as VARCHAR), '%Y%m%d')) AS \"__measure__0\"\n"
                    + "    FROM (SELECT date,\n"
                    + "          SUBSTR(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) AS date_range_1,\n"
                    + "          SUBSTR(CAST(end_date AS VARCHAR), 1, 10) AS date_range_2,\n"
                    + "          stat_period,\n"
                    + "            CASE WHEN CAST(stat_period AS VARCHAR) = 'day' THEN substr(CAST(date_parse(cast( date as VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) ELSE concat(CAST(date_format(CAST(start_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR), '~', CAST(date_format(CAST(end_date AS TIMESTAMP), '%Y%m%d') AS VARCHAR)) END AS stat_date,\n"
                    + "          area,\n"
                    + "          vid,\n"
                    + "          project,\n"
                    + "          company_id AS cid,\n"
                    + "          company_name AS company,\n"
                    + "          customer_level AS level,\n"
                    + "          vid_internal_industry_en AS industry,\n"
                    + "          vid_use_case_cn,\n"
                    + "          csdc_owner,\n"
                    + "          os,\n"
                    + "          ver,\n"
                    + "          ver_type,\n"
                    + "          product_level1,\n"
                    + "          product_level2,\n"
                    + "          product_level3,\n"
                    + "          total,\n"
                    + "          audio,\n"
                    + "          video,\n"
                    + "          video_sd,\n"
                    + "          video_hd,\n"
                    + "          video_hdp,\n"
                    + "            host_audio + host_video AS host,\n"
                    + "          host_audio,\n"
                    + "          host_video,\n"
                    + "          host_sd,\n"
                    + "          host_hd,\n"
                    + "          host_hdp,\n"
                    + "            audience_audio + audience_video AS audience,\n"
                    + "            audience_audio_low_latency + audience_video_low_latency AS audience_low_latency,\n"
                    + "          audience_audio,\n"
                    + "          audience_audio_low_latency,\n"
                    + "          audience_video,\n"
                    + "          audience_video_low_latency,\n"
                    + "          audience_sd,\n"
                    + "          audience_sd_low_latency,\n"
                    + "          audience_hd,\n"
                    + "          audience_hd_low_latency,\n"
                    + "          audience_hdp,\n"
                    + "          audience_hdp_low_latency,\n"
                    + "          total_last1,\n"
                    + "          audio_last1,\n"
                    + "          video_last1,\n"
                    + "          video_sd_last1,\n"
                    + "          video_hd_last1,\n"
                    + "          video_hdp_last1,\n"
                    + "            host_audio_last1 + host_video_last1 AS host_last1,\n"
                    + "          host_audio_last1,\n"
                    + "          host_video_last1,\n"
                    + "            audience_audio_last1 + audience_video_last1 AS audience_last1,\n"
                    + "            audience_audio_low_latency_last1 + audience_video_low_latency_last1 AS audience_low_latency_last1,\n"
                    + "          audience_audio_last1,\n"
                    + "          audience_audio_low_latency_last1,\n"
                    + "          audience_video_last1,\n"
                    + "          audience_video_low_latency_last1,\n"
                    + "          total_last7,\n"
                    + "          audio_last7,\n"
                    + "          video_last7,\n"
                    + "          video_sd_last7,\n"
                    + "          video_hd_last7,\n"
                    + "          video_hdp_last7,\n"
                    + "            host_audio_last7 + host_video_last7 AS host_last7,\n"
                    + "          host_audio_last7,\n"
                    + "          host_video_last7,\n"
                    + "            audience_audio_last7 + audience_video_last7 AS audience_last7,\n"
                    + "            audience_audio_low_latency_last7 + audience_video_low_latency_last7 AS audience_low_latency_last7,\n"
                    + "          audience_audio_last7,\n"
                    + "          audience_audio_low_latency_last7,\n"
                    + "          audience_video_last7,\n"
                    + "          audience_video_low_latency_last7\n"
                    + "        FROM report_datahub.vendor_vid_usage_di_1\n"
                    + "        WHERE date_parse(cast( date as VARCHAR), '%Y%m%d') >= DATE('2021-11-06') AND date_parse(cast( date as VARCHAR), '%Y%m%d') <= DATE('2021-12-06') AND CAST(vid AS VARCHAR) = CAST(428342 AS VARCHAR) AND dim = 1) AS \"自定义 SQL 查询\"\n"
                    + "    HAVING COUNT(1) > 0) AS \"t0\"\n"
                    + "WHERE date_parse(cast( \"自定义 SQL 查询\".\"date\" as VARCHAR), '%Y%m%d') = \"t0\".\"__measure__0\"\n"
                    + "HAVING COUNT(1) > 0";

    final String expectSql =
            "SELECT SUM(t0.audience_low_latency_last1) TEMP(Calculation_230316907702542336)(2007846651)(0), SUM(t0.audience_low_latency) TEMP(Calculation_230316907702542336)(2627877912)(0), SUM(t0.total) TEMP(Calculation_5203557528859017219)(2253843611)(0), SUM(t0.total_last1) TEMP(Calculation_5203557528859017219)(3076278274)(0), SUM(t0.total) TEMP(Calculation_5203557528862519300)(2253843611)(0), SUM(t0.total_last7) TEMP(Calculation_5203557528862519300)(431428601)(0), SUM(t0.audio_last1) TEMP(Calculation_5203557528863870981)(2207581090)(0), SUM(t0.audio) TEMP(Calculation_5203557528863870981)(4234702922)(0), SUM(t0.audio_last7) TEMP(Calculation_5203557528863973382)(1745737303)(0), SUM(t0.audio) TEMP(Calculation_5203557528863973382)(4234702922)(0), SUM(t0.video_last1) TEMP(Calculation_5203557528864096263)(1192334319)(0), SUM(t0.video) TEMP(Calculation_5203557528864096263)(3647352276)(0), SUM(t0.video_last7) TEMP(Calculation_5203557528864174088)(3553322618)(0), SUM(t0.video) TEMP(Calculation_5203557528864174088)(3647352276)(0), SUM(t0.audience) TEMP(Calculation_534802460955746306)(1306364424)(0), SUM(t0.audience_last1) TEMP(Calculation_534802460955746306)(2505681848)(0), SUM(t0.audience) TEMP(Calculation_534802460955955203)(1306364424)(0), SUM(t0.audience_last7) TEMP(Calculation_534802460955955203)(2563414513)(0), SUM(t0.audience_audio) TEMP(Calculation_534802460956037124)(1868547182)(0), SUM(t0.audience_audio_last1) TEMP(Calculation_534802460956037124)(2218970377)(0), SUM(t0.audience_audio_last7) TEMP(Calculation_534802460956184581)(1783975840)(0), SUM(t0.audience_audio) TEMP(Calculation_534802460956184581)(1868547182)(0), SUM(t0.audience_video_last1) TEMP(Calculation_534802460956319750)(188915252)(0), SUM(t0.audience_video) TEMP(Calculation_534802460956319750)(2182583436)(0), SUM(t0.audience_video_last7) TEMP(Calculation_534802460956430343)(1897793420)(0), SUM(t0.audience_video) TEMP(Calculation_534802460956430343)(2182583436)(0), MAX(t0.stat_date) TEMP(attr_stat_date_nk)(2102638629)(0), MIN(t0.stat_date) TEMP(attr_stat_date_nk)(2790680713)(0), SUM(t0.audience_audio_low_latency_last1) TEMP(audience_audio_last1_rate (复制)_230316907704033282)(67, SUM(t0.audience_audio_low_latency) TEMP(audience_audio_last1_rate (复制)_230316907704033282)(83, SUM(t0.host_audio_last1) TEMP(audience_audio_last1_rate (复制)_534802460956618761)(24, SUM(t0.host_audio) TEMP(audience_audio_last1_rate (复制)_534802460956618761)(36, SUM(t0.audience_audio_low_latency_last7) TEMP(audience_audio_last7_rate (复制)_230316907704053763)(29, SUM(t0.audience_audio_low_latency) TEMP(audience_audio_last7_rate (复制)_230316907704053763)(83, SUM(t0.host_audio_last7) TEMP(audience_audio_last7_rate (复制)_534802460956631050)(15, SUM(t0.host_audio) TEMP(audience_audio_last7_rate (复制)_534802460956631050)(36, SUM(t0.host_last1) TEMP(audience_last1_rate (复制)_534802460956647435)(15035501, SUM(t0.host) TEMP(audience_last1_rate (复制)_534802460956647435)(24867219, SUM(t0.host_last7) TEMP(audience_last7_date (复制)_534802460957310992)(24217528, SUM(t0.host) TEMP(audience_last7_date (复制)_534802460957310992)(24867219, SUM(t0.audience_low_latency_last7) TEMP(audience_low_latency_last1_rate (复制)_2303169077032461, SUM(t0.audience_low_latency) TEMP(audience_low_latency_last1_rate (复制)_2303169077032468, SUM(t0.audience_video_low_latency) TEMP(audience_low_latency_last1_rate (复制)_2303169077218301, SUM(t0.audience_video_low_latency_last1) TEMP(audience_low_latency_last1_rate (复制)_2303169077218304, SUM(t0.audience_video_low_latency) TEMP(audience_low_latency_last7_rate (复制)_2303169077218181, SUM(t0.audience_video_low_latency_last7) TEMP(audience_low_latency_last7_rate (复制)_2303169077218182, SUM(t0.host_video) TEMP(audience_video_last1_rate (复制)_534802460956696589)(26, SUM(t0.host_video_last1) TEMP(audience_video_last1_rate (复制)_534802460956696589)(42, SUM(t0.host_video_last7) TEMP(audience_video_last7_rate (复制)_534802460956704782)(21, SUM(t0.host_video) TEMP(audience_video_last7_rate (复制)_534802460956704782)(26, SUM(t0.audience_video_low_latency) sum_audience_video_low_latency_ok\n"
                    + "FROM (SELECT area, audience_audio + audience_video audience, audience_audio, audience_audio_last1, audience_audio_last7, audience_audio_low_latency, audience_audio_low_latency_last1, audience_audio_low_latency_last7, audience_hd, audience_hd_low_latency, audience_hdp, audience_hdp_low_latency, audience_audio_last1 + audience_video_last1 audience_last1, audience_audio_last7 + audience_video_last7 audience_last7, audience_audio_low_latency + audience_video_low_latency audience_low_latency, audience_audio_low_latency_last1 + audience_video_low_latency_last1 audience_low_latency_last1, audience_audio_low_latency_last7 + audience_video_low_latency_last7 audience_low_latency_last7, audience_sd, audience_sd_low_latency, audience_video, audience_video_last1, audience_video_last7, audience_video_low_latency, audience_video_low_latency_last1, audience_video_low_latency_last7, audio, audio_last1, audio_last7, company_id cid, company_name company, csdc_owner, date, SUBSTR(CAST(date_parse(CAST(date AS VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) date_range_1, SUBSTR(CAST(end_date AS VARCHAR), 1, 10) date_range_2, host_audio + host_video host, host_audio, host_audio_last1, host_audio_last7, host_hd, host_hdp, host_audio_last1 + host_video_last1 host_last1, host_audio_last7 + host_video_last7 host_last7, host_sd, host_video, host_video_last1, host_video_last7, vid_internal_industry_en industry, customer_level level, os, product_level1, product_level2, product_level3, project, CASE WHEN stat_period = 'day' THEN substr(CAST(date_parse(CAST(date AS VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) ELSE concat(CAST(date_format(CAST(start_date AS TIMESTAMP(0)), '%Y%m%d') AS VARCHAR), '~', CAST(date_format(CAST(end_date AS TIMESTAMP(0)), '%Y%m%d') AS VARCHAR)) END stat_date, stat_period, total, total_last1, total_last7, ver, ver_type, vid, vid_use_case_cn, video, video_hd, video_hd_last1, video_hd_last7, video_hdp, video_hdp_last1, video_hdp_last7, video_last1, video_last7, video_sd, video_sd_last1, video_sd_last7\n"
                    + "FROM report_datahub.vendor_vid_usage_di_1\n"
                    + "WHERE date_parse(CAST(date AS VARCHAR), '%Y%m%d') >= DATE('2021-11-06') AND date_parse(CAST(date AS VARCHAR), '%Y%m%d') <= DATE('2021-12-06') AND CAST(vid AS VARCHAR) = CAST(428342 AS VARCHAR) AND dim = 1) t0\n"
                    + "CROSS JOIN (SELECT MAX(date_parse(CAST(date AS VARCHAR), '%Y%m%d')) __measure__0\n"
                    + "FROM report_datahub.vendor_vid_usage_di_1\n"
                    + "WHERE date_parse(CAST(date AS VARCHAR), '%Y%m%d') >= DATE('2021-11-06') AND date_parse(CAST(date AS VARCHAR), '%Y%m%d') <= DATE('2021-12-06') AND CAST(vid AS VARCHAR) = CAST(428342 AS VARCHAR) AND dim = 1\n"
                    + "HAVING COUNT(*) > 0) t5\n"
                    + "WHERE date_parse(CAST(t0.date AS VARCHAR), '%Y%m%d') = t5.__measure__0\n"
                    + "HAVING COUNT(*) > 0 OR COUNT(*) > 0";
}
