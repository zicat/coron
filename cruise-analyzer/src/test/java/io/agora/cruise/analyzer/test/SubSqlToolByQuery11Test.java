package io.agora.cruise.analyzer.test;

import io.agora.cruise.analyzer.SqlAnalyzer;
import io.agora.cruise.analyzer.sql.SqlIterable;
import io.agora.cruise.analyzer.sql.SqlIterator;
import io.agora.cruise.analyzer.sql.SqlJsonIterable;
import io.agora.cruise.analyzer.sql.SqlJsonIterator;
import io.agora.cruise.core.util.Tuple2;
import io.agora.cruise.parser.sql.shuttle.Int2BooleanConditionShuttle;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** SubSqlToolByQuery2Test. */
public class SubSqlToolByQuery11Test {

    public static void main(String[] args) {
        test();
    }

    public static void test() {

        SqlJsonIterator.JsonParser parser = jsonNode -> jsonNode.get("presto_sql").asText();
        SqlIterable source = new SqlJsonIterable("/Users/zj/Desktop/query_20.json", parser);
        SqlIterable target = new SqlJsonIterable("/Users/zj/Desktop/query_21.json", parser);
        QueryTestBase queryTestBase = new QueryTestBase();
        SqlAnalyzer sqlAnalyzer = queryTestBase.createSubSqlTool(source, target);
        Map<String, RelNode> viewQueryMap = sqlAnalyzer.start();
        viewQueryMap.forEach(queryTestBase::addMaterializedView);

        AtomicInteger total = new AtomicInteger(0);
        AtomicInteger matched = new AtomicInteger(0);
        Set<String> allMatchedView = new ConcurrentSkipListSet<>();

        SqlIterator iterator =
                new SqlJsonIterable("/Users/zj/Desktop/query_22.json", parser).sqlIterator();
        int concurrent = Runtime.getRuntime().availableProcessors();
        List<Future<?>> fs = new ArrayList<>(concurrent);
        ExecutorService executorService = Executors.newFixedThreadPool(concurrent);
        final Set<String> viewSqlSet = viewQueryMap.keySet();
        while (iterator.hasNext()) {
            String querySql = iterator.next();
            System.out.println("start to check materialized view " + iterator.currentOffset());
            fs.add(
                    executorService.submit(
                            () -> {
                                try {
                                    if (queryTestBase.sqlFilter.filter(querySql)) {
                                        return;
                                    }
                                    final RelNode relNode =
                                            queryTestBase.querySql2Rel(
                                                    querySql, new Int2BooleanConditionShuttle());
                                    final Tuple2<Set<String>, RelNode> tuple2 =
                                            queryTestBase.canMaterializedWithRelNode(
                                                    relNode, viewSqlSet);
                                    if (!tuple2.f0.isEmpty()) {
                                        matched.incrementAndGet();
                                        allMatchedView.addAll(tuple2.f0);
                                    }
                                    total.incrementAndGet();
                                } catch (Exception e) {
                                    queryTestBase.exceptionHandler.handle(querySql, e);
                                }
                            }));
            if (fs.size() >= concurrent) {
                fs.forEach(
                        f -> {
                            try {
                                f.get();
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        });
                fs.clear();
            }
        }
        fs.forEach(
                f -> {
                    try {
                        f.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });
        fs.clear();
        executorService.shutdownNow();
        System.out.println("===========matched view================");
        for (String viewName : allMatchedView) {
            System.out.println(queryTestBase.toSql(viewQueryMap.get(viewName)));
            System.out.println("----------------------------------------------");
        }
        System.out.println(
                "total:" + total + ",matched:" + matched + ",view count:" + allMatchedView.size());
    }
}
