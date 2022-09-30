### 发布打包
```
mvn clean package -DlibScope=provided -Dmaven.test.skip=true
```

### 部署

解压 assembly/target/ 目录下生成可用包 datatunnel-[version].tar.gz。复制所有jar 到 spark_home/jars 
在conf/spark-default.conf 添加配置: spark.sql.extensions com.github.melin.datatunnel.core.DataxExtensions

### dtunnel sql 语法
```sql
datatunnel source('数据类型名称') options(键值对参数) 
    transform(数据加工SQL，可以对数据处理后输出)
    sink('数据类型名称') options(键值对参数)
```

### 支持数据源

| 数据源           | Reader(读) | Writer(写)    | 文档                                                                                   |
|:--------------|:----------| :------      |:-------------------------------------------------------------------------------------|
| file          | √         | √            | [读写](doc/file.md) 支持excel, json，csv 文件                                               |
| sftp          | √         | √            | [读写](doc/sftp.md)                                                                    |
| hdfs          | √         |              | [读](doc/hdfs.md)                                                                     |
| jdbc          | √         | √            | [读写](doc/tispark.md) 支持 DB2、MySQL、MS Sql、Oracle、PostgreSQL、TIDB、Hana、Greenplum、Gauss |
| tispark       |           | √            | [读](doc/tispark.md) 基于TiSpark读取数据                                                    |
| hive          | √         | √            | [读写](doc/hive.md)                                                                    |
| hbase         |           | √            | [写](doc/hbase.md)                                                                    |
| clickhouse    | √         | √            | [读写](doc/clickhouse.md) 基于 spark-clickhouse-connector 项目                             |
| elasticsearch |           | √            | 开发中                                                                                  |
| log           |           | √            | [写](doc/log.md)                                                                      |
| kafka         | √         | √            | [读写](doc/kafka.md) spark streaming任务，支持写入jdbc，hudi表                                  |
| doris         | √         | √            | [读写](doc/doris.md) 基于 doris-spark-connector                                          |

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

### 参考

1. [Bucket4j 限流库](https://github.com/vladimir-bukhtoyarov/bucket4j)
2. https://github.com/housepower/spark-clickhouse-connector
3. https://github.com/apache/incubator-seatunnel
