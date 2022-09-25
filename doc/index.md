DataTunnel 两个不同数据源之间交换同步数据，具体语法：

### DataTunnel sql 语法
```sql
datatunnel source('数据类型名称') options(键值对参数) 
    transform(数据加工SQL，可以对数据处理后输出)
    sink('数据类型名称') options(键值对参数)
```

### 支持数据源

| 数据源           | Reader(读)  | Writer(写)    | 文档                   |
|:--------------| :-----      | :------      |:---------------------|
| sftp          | √           | √            | [读写](sftp.html)      |
| hdfs          | √           |              | [读](hdfs.html)       |
| jdbc          | √           | √            | [读写](jdbc.html)      |
| hive          | √           | √            | [读写](hive.html)      |
| hbase         |             | √            | [写](hbase.html)      |
| clickhouse    |             | √            | [写](clickhouse.html) |
| elasticsearch |             | √            | 开发中                  |
| log           |             | √            | [写](log.html)        |
| kafka         |             | √            | [写](kafka.html)      |

### example
```sql
DATATUNNEL SOURCE("mysql") OPTIONS (
  username = "dataworks",
  password = "dataworks2021",
  host = '10.5.20.20',
  port = 3306,
  databaseName = 'dataworks',
  tableName = 'dc_dtunnel_datasource',
  columns = ["*"]
)
SINK("hive") OPTIONS (
  databaseName = "bigdata",
  tableName = 'hive_dtunnel_datasource',
  writeMode = 'overwrite',
  columns = ["*"]
);

DATATUNNEL SOURCE('mysql') OPTIONS (
  username = 'dataworks',
  password = 'dataworks2021',
  host = '10.5.20.20',
  port = 3306,
  resultTableName = 'tdl_dc_job',
  databaseName = 'dataworks',
  tableName = 'dc_job',
  columns = ['*']
)
TRANSFORM = 'select * from tdl_dc_job where type="spark_sql"'
SINK('log') OPTIONS (
  numRows = 10
);

DATATUNNEL SOURCE("hive") OPTIONS (
  databaseName = 'bigdata',
  tableName = 'hive_dtunnel_datasource',
  columns = ['id', 'code', 'type', 'description', 'config', 'gmt_created', 'gmt_modified', 'creater', 'modifier']
)
SINK("mysql") OPTIONS (
  username = "dataworks",
  password = "dataworks2021",
  host = '10.5.20.20',
  port = 3306,
  databaseName = 'dataworks',
  tableName = 'dc_datax_datasource_copy1',
  writeMode = 'overwrite',
  truncate = true,
  columns = ['id', 'code', 'dstype', 'description', 'config', 'gmt_created', 'gmt_modified', 'creater', 'modifier']
)
```
