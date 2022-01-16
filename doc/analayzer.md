# SqlAnalyzer
## What is SqlAnalyzer
SqlAnalyzer is a tool which can find all subQuery from two sql iterators, and balance the performance and completeness, Support running with multi threads.
## Example
- Get Starting
    - Simple Java Code
        ```java
        public static void main(String[] args) throws SqlParseException {
            //create context and register table meta. create SqlAnalyzer by context and Dialect
            final String ddl = "CREATE TABLE default_db.test(d1 varchar, d2 varchar, m1 int, m2 bigint)";
            final CalciteContext context = new CalciteContext("default_db").addTables(ddl);
            final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer(context, PrestoDialect.DEFAULT);
    
            final String query1 = "SELECT d1, sum(m1) AS s1 FROM default_db.test WHERE d1 = 'aaa' GROUP BY d1";
            final String query2 = "SELECT d1, sum(m2) AS s2 FROM default_db.test WHERE d1 = 'bbb' GROUP BY d1";
            final SqlIterable source = new SqlListIterable(Collections.singletonList(query1));
            final SqlIterable target = new SqlListIterable(Collections.singletonList(query2));
          
            // start to analyzer by SqlAnalyzer with multi threads
            final int threadCount = 10;
            final Map<String, RelNode> result =  sqlAnalyzer.analyze(source, target, threadCount);  
            result.forEach((k, v) -> System.out.println(context.toSql(v)));
      
            //register public sub sql and materialized view and rewrite query sql
            result.forEach(context::addMaterializedView);
            Pair<RelNode, List<RelOptMaterialization>> rewriteQuery1Result = context.materializedViewOpt(context.querySql2Rel(query1));
            Pair<RelNode, List<RelOptMaterialization>> rewriteQuery2Result = context.materializedViewOpt(context.querySql2Rel(query2));
            System.out.println("=======================");
            System.out.println(context.toSql(rewriteQuery1Result.left));
            System.out.println("-----------------------");
            System.out.println(context.toSql(rewriteQuery2Result.left));
        }
        ```
    - Result  
        ```text
        SELECT d1, SUM(m1) s1, SUM(m2) s2
        FROM default_db.test
        WHERE d1 = 'bbb' OR d1 = 'aaa'
        GROUP BY d1
        =======================
        SELECT d1, s1
        FROM default_db.materialized_view_1
        WHERE d1 = 'aaa'
        -----------------------
        SELECT d1, s2
        FROM default_db.materialized_view_1
        WHERE d1 = 'bbb'
        ```
- RelShuttleChain Demo

    RelShuttleChain can adjust the struct of RelNode before analysis, The most common usage is to rollup partition field in filter.
    
    - Create SqlAnalyzer with RelShuttleChain
        ```java
        public static void main(String[] args) throws SqlParseException {
            //create context and register table meta.
            final String ddl = "CREATE TABLE default_db.test(d1 varchar, d2 varchar, m1 int, m2 bigint, date varchar)";
            final CalciteContext context = new CalciteContext("default_db").addTables(ddl);
            final SqlAnalyzer sqlAnalyzer = new SqlAnalyzer(context, PrestoDialect.DEFAULT) {
                @Override
                protected RelShuttleChain createShuttleChain(RelNode relNode) {
                    // if relnode contains table default_db.test, use PartitionRelShuttle to adjust partition field
                    if(TableRelShuttleImpl.tables(relNode).contains("default_db.test")) {
                        return RelShuttleChain.of(FilterRexNodeRollUpShuttle.partitionShuttles("date"));
                    }
                    return super.createShuttleChain(relNode);
                }
            };
    
            final String query1 = "SELECT d1, sum(m1) AS s1 FROM default_db.test WHERE d1 = 'aaa' and date='2022-01-01' GROUP BY d1, date";
            final String query2 = "SELECT d1, sum(m2) AS s2 FROM default_db.test WHERE d1 = 'bbb' and date='2022-01-02' GROUP BY d1, date";
            final SqlIterable source = new SqlListIterable(Collections.singletonList(query1));
            final SqlIterable target = new SqlListIterable(Collections.singletonList(query2));
            final int threadCount = 10;
            final Map<String, RelNode> result =  sqlAnalyzer.analyze(source, target, threadCount);
            result.forEach((k, v) -> System.out.println(context.toSql(v)));
            result.forEach(context::addMaterializedView);
            // convert sql query to relNode.
    
            Pair<RelNode, List<RelOptMaterialization>> rewriteQuery1Result = context.materializedViewOpt(context.querySql2Rel(query1));
            Pair<RelNode, List<RelOptMaterialization>> rewriteQuery2Result = context.materializedViewOpt(context.querySql2Rel(query2));
            System.out.println("=======================");
            System.out.println(context.toSql(rewriteQuery1Result.left));
            System.out.println("-----------------------");
            System.out.println(context.toSql(rewriteQuery2Result.left));
        }   
        ```
    - Result  
        ```text
        SELECT d1, date tmp_p_5, SUM(m1) s1, SUM(m2) s2
        FROM default_db.test
        WHERE d1 = 'bbb' OR d1 = 'aaa'
        GROUP BY d1, date
        =======================
        SELECT d1, SUM(s1) s1
        FROM default_db.view_0
        WHERE d1 = 'aaa' AND tmp_p_5 = '2022-01-01'
        GROUP BY d1
        -----------------------
        SELECT d1, SUM(s2) s2
        FROM default_db.view_0
        WHERE d1 = 'bbb' AND tmp_p_5 = '2022-01-02'
        GROUP BY d1
        ```