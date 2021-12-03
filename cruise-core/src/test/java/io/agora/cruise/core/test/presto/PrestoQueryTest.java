package io.agora.cruise.core.test.presto;

import com.csvreader.CsvReader;
import io.agora.cruise.core.ResultNode;
import io.agora.cruise.core.test.NodeRelTest;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
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
public class PrestoQueryTest extends NodeRelTest {

    @Test
    public void testJoin() throws SqlParseException {

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

        final SqlNode sqlNode1 = SqlNodeTool.toQuerySqlNode(sql1);
        final SqlNode sqlNode2 = SqlNodeTool.toQuerySqlNode(sql2);
        final RelNode relNode1 = createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
        final RelNode relNode2 = createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

        ResultNode<RelNode> resultNode =
                findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));
        assertResultNode(expectSql, resultNode);
    }

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
                        createSqlToRelConverter().convertQuery(sqlNode1, true, true).rel;
                final RelNode relNode2 =
                        createSqlToRelConverter().convertQuery(sqlNode2, true, true).rel;

                ResultNode<RelNode> resultNode =
                        findSubNode(createNodeRelRoot(relNode1), createNodeRelRoot(relNode2));

                if (!resultNode.isEmpty() && toSql(resultNode).contains("GROUP BY")) {
                    System.out.println("==================== similar sql:");
                    System.out.println(toSql(resultNode));
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

    public PrestoQueryTest() throws SqlParseException {
        String ddl1 =
                "CREATE TABLE report_datahub.agora_vid_usage_cube_di (\n"
                        + "    group_mark varchar,\n"
                        + "    product_line varchar,\n"
                        + "    product_type varchar,\n"
                        + "    client_type varchar,\n"
                        + "    vid varchar,\n"
                        + "    company_id varchar,\n"
                        + "    industry varchar,\n"
                        + "    domain varchar,\n"
                        + "    country varchar,\n"
                        + "    isp varchar,\n"
                        + "    os varchar,\n"
                        + "    ver varchar,\n"
                        + "    net varchar,\n"
                        + "    app_type varchar,\n"
                        + "    is_new_vendor varchar,\n"
                        + "    device_vendor varchar,\n"
                        + "    device_type varchar,\n"
                        + "    is_100pcu_channel varchar,\n"
                        + "    browser_type varchar,\n"
                        + "    audio_video double,\n"
                        + "    host double,\n"
                        + "    audience double,\n"
                        + "    audio double,\n"
                        + "    host_audio double,\n"
                        + "    audience_audio double,\n"
                        + "    video double,\n"
                        + "    video_sd double,\n"
                        + "    host_video_sd double,\n"
                        + "    audience_video_sd double,\n"
                        + "    video_hd double,\n"
                        + "    host_video_hd double,\n"
                        + "    audience_video_hd double,\n"
                        + "    video_hdp double,\n"
                        + "    host_video_hdp double,\n"
                        + "    audience_video_hdp double,\n"
                        + "    _tidb_rowid bigint,\n"
                        + "    date integer\n"
                        + " )";
        String ddl2 =
                "CREATE TABLE report_datahub.new_vendor_dimension (\n"
                        + "   email varchar,\n"
                        + "   date timestamp(3),\n"
                        + "   projectname varchar,\n"
                        + "   companyname varchar,\n"
                        + "   companyid bigint,\n"
                        + "   vid bigint,\n"
                        + "   industry varchar,\n"
                        + "   country_abb varchar,\n"
                        + "   country varchar,\n"
                        + "   cn_us varchar,\n"
                        + "   vidfirstusagetime timestamp(3),\n"
                        + "   vidlastusagetime timestamp(3),\n"
                        + "   vidrtmfirstusagetime timestamp(3),\n"
                        + "   vidrtmlastusagetime timestamp(3),\n"
                        + "   vidrtm1000firstusagetime timestamp(3),\n"
                        + "   vidrtm1000lastusagetime timestamp(3),\n"
                        + "   vidrtm10000firstusagetime timestamp(3),\n"
                        + "   vidrtm10000lastusagetime timestamp(3),\n"
                        + "   salesemail varchar,\n"
                        + "   issendmsg integer,\n"
                        + "   isverify integer,\n"
                        + "   verifyphone varchar,\n"
                        + "   mobile varchar,\n"
                        + "   registersource integer,\n"
                        + "   registerstep integer,\n"
                        + "   sourcecategory varchar,\n"
                        + "   sourcetype varchar,\n"
                        + "   signupos varchar,\n"
                        + "   projectcreated varchar,\n"
                        + "   projectupdated varchar,\n"
                        + "   accountcreated varchar,\n"
                        + "   accountupdated varchar,\n"
                        + "   cidfirstpaidtime timestamp(3),\n"
                        + "   cidlastpaidtime timestamp(3),\n"
                        + "   verifydate varchar,\n"
                        + "   app_id varchar,\n"
                        + "   vendor_description varchar,\n"
                        + "   vendor_stage integer,\n"
                        + "   vendor_country varchar,\n"
                        + "   vendor_user varchar,\n"
                        + "   vendor_status integer,\n"
                        + "   in_use integer,\n"
                        + "   parent_id integer,\n"
                        + "   max_channels integer,\n"
                        + "   start_time varchar,\n"
                        + "   end_time varchar,\n"
                        + "   need_token integer,\n"
                        + "   project_id varchar,\n"
                        + "   in_channel_permission integer,\n"
                        + "   is_deleted integer,\n"
                        + "   csdc_industry varchar,\n"
                        + "   csdc_scene_level1 varchar,\n"
                        + "   csdc_scene_level2 varchar,\n"
                        + "   company_description varchar,\n"
                        + "   company_status integer,\n"
                        + "   customer_level varchar,\n"
                        + "   area varchar,\n"
                        + "   ios3 varchar,\n"
                        + "   province varchar,\n"
                        + "   industry_id integer,\n"
                        + "   industry_name varchar,\n"
                        + "   interest integer,\n"
                        + "   environment integer,\n"
                        + "   app_limit integer,\n"
                        + "   member_limit integer,\n"
                        + "   reseller_id bigint,\n"
                        + "   user_name varchar,\n"
                        + "   language varchar,\n"
                        + "   timezone varchar,\n"
                        + "   email_host varchar,\n"
                        + "   verify_email varchar,\n"
                        + "   verify_type integer,\n"
                        + "   account_id bigint,\n"
                        + "   account_status integer,\n"
                        + "   register_ip varchar,\n"
                        + "   register_ip_country varchar,\n"
                        + "   register_ip_province varchar,\n"
                        + "   register_ip_city varchar,\n"
                        + "   register_user_agent varchar,\n"
                        + "   mobile_register integer,\n"
                        + "   sales_id integer,\n"
                        + "   is_test_acc integer,\n"
                        + "   is_identity integer,\n"
                        + "   identity_type integer,\n"
                        + "   pi_auth_type integer,\n"
                        + "   pi_name varchar,\n"
                        + "   pi_number varchar,\n"
                        + "   pi_status integer,\n"
                        + "   pi_submit_time bigint,\n"
                        + "   pi_update_time bigint,\n"
                        + "   ci_auth_type integer,\n"
                        + "   ci_name varchar,\n"
                        + "   ci_address varchar,\n"
                        + "   ci_website varchar,\n"
                        + "   ci_phone varchar,\n"
                        + "   ci_credit_code varchar,\n"
                        + "   ci_legal_person_name varchar,\n"
                        + "   ci_legal_person_number varchar,\n"
                        + "   ci_status integer,\n"
                        + "   ci_submit_time varchar,\n"
                        + "   ci_update_time varchar,\n"
                        + "   ci_number bigint,\n"
                        + "   sf_line varchar,\n"
                        + "   sf_stage varchar,\n"
                        + "   sf_industry varchar,\n"
                        + "   sf_general_industry varchar,\n"
                        + "   sf_scene_level1 varchar,\n"
                        + "   sf_scene_level2 varchar,\n"
                        + "   sf_source varchar,\n"
                        + "   sf_online_time varchar,\n"
                        + "   sf_application_name varchar,\n"
                        + "   sf_win_back integer,\n"
                        + "   sf_cn_sa varchar,\n"
                        + "   sf_cn_tam varchar,\n"
                        + "   sf_cn_cs varchar,\n"
                        + "   sf_create_time varchar,\n"
                        + "   sf_update_time varchar,\n"
                        + "   utm_source varchar,\n"
                        + "   utm_medium varchar,\n"
                        + "   utm_campaign varchar,\n"
                        + "   utm_keyword varchar,\n"
                        + "   utm_device varchar,\n"
                        + "   company_level varchar,\n"
                        + "   csdc_company_level varchar,\n"
                        + "   csdc_company_industry varchar,\n"
                        + "   csdc_owner varchar,\n"
                        + "   csdc_is_online_vid varchar,\n"
                        + "   csdc_nickname varchar,\n"
                        + "   signkey varchar,\n"
                        + "   is_rtc_day1_remain integer,\n"
                        + "   is_rtc_day3_remain integer,\n"
                        + "   is_rtc_day7_remain integer,\n"
                        + "   is_rtc_day30_remain integer,\n"
                        + "   is_rtc_day60_remain integer,\n"
                        + "   is_rtm_day1_remain integer,\n"
                        + "   is_rtm_day3_remain integer,\n"
                        + "   is_rtm_day7_remain integer,\n"
                        + "   is_rtm_day30_remain integer,\n"
                        + "   is_rtm_day60_remain integer,\n"
                        + "   is_rtm_week1_remain integer,\n"
                        + "   is_rtm_week2_remain integer,\n"
                        + "   is_rtm_week3_remain integer,\n"
                        + "   is_rtm_week4_remain integer,\n"
                        + "   is_rtm_week8_remain integer,\n"
                        + "   use_both_token_appid integer,\n"
                        + "   vid_first_integrated_day timestamp(3),\n"
                        + "   vid_first_online_day timestamp(3),\n"
                        + "   is_explore integer,\n"
                        + "   use_case_en varchar,\n"
                        + "   use_case_cn varchar,\n"
                        + "   internal_industry_en varchar,\n"
                        + "   internal_industry_cn varchar,\n"
                        + "   vid_internal_industry_cn varchar,\n"
                        + "   vid_internal_industry_en varchar,\n"
                        + "   vid_sector_cn varchar,\n"
                        + "   vid_sector_en varchar,\n"
                        + "   csdc_owner_area varchar,\n"
                        + "   is_strategy_winback integer,\n"
                        + "   sf_lead_companyid bigint,\n"
                        + "   sf_lead_qslcreatedate timestamp(3),\n"
                        + "   sf_lead_convertedopportunityid varchar,\n"
                        + "   sf_opportunity_id varchar,\n"
                        + "   sf_opportunity_currentstage varchar,\n"
                        + "   authentication_status bigint\n"
                        + ")";
        addTables(ddl1, ddl2);
    }
}
