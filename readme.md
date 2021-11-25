# Introduce cruise
Cruise 通过calcite工具对多个Sql进行分析，并试图找出最大公共子树。
# 项目介绍
## cruise-parser
解析Spark Sql，并转化为Calcite RelNode进行后续分析。

## cruise-core
对RelNode进行匹配和查找，生成最大公共子树。