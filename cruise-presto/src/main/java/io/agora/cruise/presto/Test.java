package io.agora.cruise.presto;

import io.agora.cruise.parser.CalciteContext;
import io.agora.cruise.parser.SqlNodeTool;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.Arrays;

/** test. */
public class Test extends PrestoQueryTest {

    public static void main(String[] args) throws SqlParseException {
        CalciteContext context = context();
        final SqlNode sqlNode = SqlNodeTool.toQuerySqlNode(viewQuery);
        final SqlToRelConverter converter = context.createSqlToRelConverter();
        final RelRoot viewQueryRoot = converter.convertQuery(sqlNode, true, true);
        final HepPlanner hepPlanner =
                context.createPlanner(Arrays.asList(FilterMergeRule.Config.DEFAULT.toRule()));
        hepPlanner.setRoot(viewQueryRoot.rel);
        System.out.println(context.toSql(hepPlanner.findBestExp()));
    }

    private static String viewQuery =
            "SELECT SUM(\"s2l_25ms_delay_sec\") AS \"TEMP(Calculation_1430244791154503680)(2227145667)(0)\", SUM(\"s2l_total_delay_sec\") AS \"TEMP(Calculation_1430244791154503680)(2460078802)(0)\", SUM(\"5s_succ_join_ap_cnt\") AS \"TEMP(Calculation_1430244791247978510)(1716451703)(0)\", SUM(\"total_join_ap_cnt\") AS \"TEMP(Calculation_1430244791247978510)(2681092799)(0)\", SUM(\"video_totaltime_200\") AS \"TEMP(Calculation_2810035073237950465)(2104207895)(0)\", SUM(\"video_freeze_time_200\") AS \"TEMP(Calculation_2810035073237950465)(530162565)(0)\", SUM(\"totalJoin\") AS \"TEMP(Calculation_374009936508170240)(2073556184)(0)\", SUM(\"totalJoinSuccess\") AS \"TEMP(Calculation_374009936508170240)(2807417001)(0)\", SUM(\"threeSecJoinSuccess\") AS \"TEMP(Calculation_374009936508321793)(3693645608)(0)\", SUM(\"fiveSecJoinSuccess\") AS \"TEMP(Calculation_374009936508416002)(383616769)(0)\", SUM(\"video_totaltime\") AS \"TEMP(Calculation_374009936515215363)(2797449586)(0)\", SUM(\"video_freeze_time\") AS \"TEMP(Calculation_374009936515215363)(2940759860)(0)\", SUM(\"audio_totaltime\") AS \"TEMP(Calculation_374009936517861380)(1652566750)(0)\", SUM(\"audio_freeze_time\") AS \"TEMP(Calculation_374009936517861380)(2919389392)(0)\", SUM(\"p2s_lost_Leq5\") AS \"TEMP(Calculation_374009936519704581)(1936123547)(0)\", SUM(\"p2s_lost_all\") AS \"TEMP(Calculation_374009936519704581)(749826584)(0)\", SUM(\"s2l_75ms_delay_sec\") AS \"TEMP(Calculation_6542393318886285312)(2003933318)(0)\", SUM(\"s2l_lost_Leq5\") AS \"TEMP(Calculation_7034341207026868236)(2745840127)(0)\", SUM(\"s2l_lost_all\") AS \"TEMP(Calculation_7034341207026868236)(734589263)(0)\", SUM(\"crash_cnt\") AS \"TEMP(Calculation_7034341207372070925)(2360900687)(0)\", SUM(\"service_cnt\") AS \"TEMP(Calculation_7034341207372070925)(4291252132)(0)\", \"DATE_ADD\"('SECOND', 0, \"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d')) AS \"tdy_date_ok\"\n"
                    + "FROM (SELECT \"1s_succ_join_ap_cnt\", \"1s_succ_join_ap_cnt_last1\", \"1s_succ_join_ap_cnt_last7\", \"3s_succ_join_session_cnt_last1\", \"3s_succ_join_session_cnt_last7\", \"5s_succ_join_ap_cnt\", \"5s_succ_join_ap_cnt_last1\", \"5s_succ_join_ap_cnt_last7\", \"5s_succ_join_session_cnt_last1\", \"5s_succ_join_session_cnt_last7\", \"audio_freeze_time\", \"audio_freeze_time_last1\", \"audio_freeze_time_last7\", \"audio_totaltime\", \"audio_totaltime_last1\", \"audio_totaltime_last7\", \"cnt_sid\", \"company_id\", \"company_name\", \"country\", \"crash_cnt\", \"crash_cnt_last1\", \"crash_cnt_last7\", \"date\", \"dim\", \"domain\", \"end_date\", \"fivesecjoinsuccess\" AS \"fiveSecJoinSuccess\", \"lastusage\", \"level1\", \"level2\", \"level3\", \"level4\", \"net\", \"os\", \"p2s_lost_leq5\" AS \"p2s_lost_Leq5\", \"p2s_lost_leq5_last1\" AS \"p2s_lost_Leq5_last1\", \"p2s_lost_leq5_last7\" AS \"p2s_lost_Leq5_last7\", \"p2s_lost_all\", \"p2s_lost_all_last1\", \"p2s_lost_all_last7\", '' AS \"pd\", CASE WHEN \"stat_period\" = 'day' THEN \"substr\"(CAST(\"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d') AS VARCHAR), 1, 10) ELSE \"concat\"(CAST(\"substr\"(CAST(\"start_date\" AS VARCHAR), 1, 10) AS VARCHAR), '~', CAST(\"substr\"(CAST(\"end_date\" AS VARCHAR), 1, 10) AS VARCHAR)) END AS \"period\", \"product_level1\", \"product_level2\", \"product_level3\", \"product_level4\", '' AS \"product_line\", '' AS \"product_type\", \"project\", \"region\", \"rowsnum\", \"s2l_25ms_delay_sec\", \"s2l_25ms_delay_sec_last1\", \"s2l_25ms_delay_sec_last7\", \"s2l_75ms_delay_sec\", \"s2l_75ms_delay_sec_last1\", \"s2l_75ms_delay_sec_last7\", \"s2l_lost_leq5\" AS \"s2l_lost_Leq5\", \"s2l_lost_leq5_last1\" AS \"s2l_lost_Leq5_last1\", \"s2l_lost_leq5_last7\" AS \"s2l_lost_Leq5_last7\", \"s2l_lost_all\", \"s2l_lost_all_last1\", \"s2l_lost_all_last7\", \"s2l_total_delay_sec\", \"s2l_total_delay_sec_last1\", \"s2l_total_delay_sec_last7\", \"service_cnt\", \"service_cnt_last1\", \"service_cnt_last7\", \"start_date\", \"stat_period\", \"succ_join_session_cnt_last1\", \"succ_join_session_cnt_last7\", \"tag\", \"thisusage\", \"threesecjoinsuccess\" AS \"threeSecJoinSuccess\", \"totaljoin\" AS \"totalJoin\", \"totaljoinsuccess\" AS \"totalJoinSuccess\", \"total_join_ap_cnt\", \"total_join_ap_cnt_last1\", \"total_join_ap_cnt_last7\", \"total_join_session_cnt_last1\", \"total_join_session_cnt_last7\", \"total_session_cnt_last1\", \"total_session_cnt_last7\", \"ver\", \"ver_type\", \"vid\", \"video_freeze_time\", \"video_freeze_time_200\", \"video_freeze_time_200_last1\", \"video_freeze_time_200_last7\", \"video_freeze_time_500\", \"video_freeze_time_500_last1\", \"video_freeze_time_500_last7\", \"video_freeze_time_last1\", \"video_freeze_time_last7\", \"video_totaltime\", \"video_totaltime_200\", \"video_totaltime_200_last1\", \"video_totaltime_200_last7\", \"video_totaltime_500\", \"video_totaltime_500_last1\", \"video_totaltime_500_last7\", \"video_totaltime_last1\", \"video_totaltime_last7\"\n"
                    + "FROM \"report_datahub\".\"pub_levels_quality_di_1\"\n"
                    + "WHERE \"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d') >= \"DATE\"('2021-11-21') AND \"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d') <= \"DATE\"('2021-12-05') OR \"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d') >= \"DATE\"('2021-11-21') AND \"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d') <= \"DATE\"('2021-11-21')) AS \"t0\"\n"
                    + "WHERE CASE WHEN \"tag\" = 'Industry' AND \"product_level1\" = 'RTC' AND \"dim\" = 'osver' THEN FALSE WHEN \"tag\" = 'VerType' AND \"product_level1\" = 'RTC' AND \"dim\" = 'osver' THEN FALSE WHEN \"tag\" = 'Product' AND \"product_level1\" = 'RTC' AND \"dim\" = 'osver' THEN TRUE ELSE FALSE END\n"
                    + "GROUP BY \"DATE_ADD\"('SECOND', 0, \"date_parse\"(CAST(\"date\" AS VARCHAR), '%Y%m%d'))";
}
