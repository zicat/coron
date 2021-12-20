# Introduce cruise
Cruise is a tool to find the max-public sub tree on query list, and create the calcite materialized view query automatically. 
# Introduce sub modules
## cruise-parser
Provide CalciteContext to convert sql to RelNode conveniently. 

## cruise-core
Find the max-public sub tree on two RelNodes.

## cruise-analyzer
Check max-public sub tree whether can convert materialized view query and replace the query, filter useless sub tree 