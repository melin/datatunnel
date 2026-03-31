### mysql source
从 mysql 表读取数据，如果添加过滤条件，spark jdbc 切分片，生成的 select sql，是嵌套查询，需要确保切分字段有索引，且 derived_merge 开启。
```sql
--确保 derived_merge = on
SELECT @@optimizer_switch
```

### 参考
https://dzlab.github.io/spark/2022/02/10/spark-jdbc-partitioning/

