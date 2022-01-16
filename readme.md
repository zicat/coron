# Introduce Coron
Coron is a tool to find the max-public sub tree on query list, and create the calcite materialized view query automatically. 
# Introduce sub modules
## coron-parser
Provide CalciteContext to convert sql to RelNode conveniently. 

## coron-core
Find the max-public sub tree on two RelNodes.

## coron-analyzer
Check max-public sub tree whether can convert materialized view query and replace the query, filter useless sub tree 

# Example
-  Compile and Install Project
   ```shell
   $ git clone https://github.com/zicat/coron.git
   $ cd coron && maven clean install
    ```
-  Import dependency by maven like below
    ```xml
    <dependency>
        <artifactId>coron-analyzer</artifactId>
        <groupId>org.zicat</groupId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
    ```
- Java code example
    ```java
    public static void main(String[] args) throws SqlParseException {
        //create context and register table meta.
        final String ddl = "CREATE TABLE default_db.test(d1 varchar, d2 varchar, m1 int, m2 bigint)";
        final CalciteContext context = new CalciteContext("default_db").addTables(ddl);

        // convert sql query to relNode.
        final String query1 = "SELECT d1, sum(m1) AS s1 FROM default_db.test WHERE d1 = 'aaa' GROUP BY d1";
        final String query2 = "SELECT d1, sum(m2) AS s2 FROM default_db.test WHERE d1 = 'bbb' GROUP BY d1";
        final RelNode relNode1 = context.querySql2Rel(query1);
        final RelNode relNode2 = context.querySql2Rel(query2);

        //find the public sub sql and print
        final ResultNode<RelNode> resultNode = NodeUtils.findFirstSubNode(
                NodeUtils.createNodeRelRoot(relNode1), NodeUtils.createNodeRelRoot(relNode2));
        System.out.println(context.toSql(resultNode.getPayload()));

        //register public sub sql and materialized view and rewrite query sql
        context.addMaterializedView("materialized_view_1", resultNode.getPayload());
        Pair<RelNode, List<RelOptMaterialization>> rewriteQuery1Result = context.materializedViewOpt(relNode1);
        Pair<RelNode, List<RelOptMaterialization>> rewriteQuery2Result = context.materializedViewOpt(relNode2);
        System.out.println("=======================");
        System.out.println(context.toSql(rewriteQuery1Result.left));
        System.out.println("-----------------------");
        System.out.println(context.toSql(rewriteQuery2Result.left));
    }
    ```   
 - Show result
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
 - [Learning more](doc/analayzer.md)  