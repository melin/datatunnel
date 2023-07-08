## 发布打包

#### 独立集成spark 打包
```
-- antlr4 版本要与spark 中版本一致
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Dantlr4.version=4.8 -Pcdh6
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Dantlr4.version=4.8 -Phadoop3
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Dantlr4.version=4.8 -Phadoop2
```

#### superior 平台打包，排除一些平台已经有的jar
```
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Pcdh6
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Phadoop3
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Phadoop2
```

### 构建AWS EMR镜像
```
docker buildx build --platform linux/amd64 -t emr6.9-serverless-spark .
docker tag emr6.9-serverless-spark:latest public.ecr.aws/w6m0k7l2/emr6.9-serverless-spark:latest
docker push public.ecr.aws/w6m0k7l2/emr6.9-serverless-spark:latest
```


## 部署

解压 assembly/target/ 目录下生成可用包 datatunnel-[version].tar.gz。复制所有jar 到 spark_home/jars 
在conf/spark-default.conf 添加配置: spark.sql.extensions com.github.melin.datatunnel.core.DataxExtensions

## datatunnel sql 语法

启动 ./bin/spark-sql，可以直接执行如下SQL 语法

```sql
-- hive source 支持CTE语法，方便原表数据经过处理过，写入到目标表，其他数据源不支持CTE 语法。
-- 相比 transform 更灵活
WITH t AS (
    WITH t2 AS (SELECT 1)
    SELECT * FROM t2
)
datatunnel source('数据源类型名称') options(键值对参数) 
    transform(数据加工SQL，可以对数据处理后输出)
    sink('数据源类型名称') options(键值对参数)
```

```sql
-- 查看不同数据源 options 参数，如果指定SOURCE，只输出数据source options参数，如果指定SINK，只输出数据sink options参数。如果输出为空，说明不支持source或者sink
DATATUNNEL HELP (SOURCE | SINK | ALL) ('数据源类型名称')

```

## 支持数据源

| 数据源           | Reader(读) | Writer(写)    | 文档                                                                               |
|:--------------|:----------| :------      |:---------------------------------------------------------------------------------|
| file          | √         | √            | [读写](doc/file.md) 支持excel, json，csv, parquet、orc、text 文件                          |
| sftp          | √         | √            | [读写](doc/sftp.md) 支持excel, json，csv, parquet、orc、text 文件                         |                       
| ftp           | √         | √            | [读写](doc/ftp.md)  支持excel, json，csv, parquet、orc、text 文件                         |
| s3            | √         | √            | [读写](doc/s3.md)  支持excel, json，csv, parquet、orc、text 文件                          |
| hdfs          | √         |              | [读](doc/hdfs.md) 支持excel, json，csv, parquet、orc、text 文件                          |
| jdbc          | √         | √            | [读写](doc/jdbc.md) 支持: mysql，oracle，db2，sqlserver，hana，tidb，guass，postgresql,tidb |
| hive          | √         | √            | [读写](doc/hive.md)                                                                |
| clickhouse    | √         | √            | [读写](doc/clickhouse.md) 基于 spark-clickhouse-connector 项目                         |
| cassandra     | √         | √            | [读写](doc/cassandra.md)                                                           |
| elasticsearch |           | √            | [读写](doc/elasticsearch.md) elasticsearch 7 版本                                    |
| log           |           | √            | [写](doc/log.md)                                                                  |
| kafka         | √         | √            | [读写](doc/kafka.md) spark streaming任务，支持写入jdbc，hudi表                              |
| doris         | √         | √            | [读写](doc/doris.md) 基于 doris-spark-connector                                      |
| starrocks     | √         | √            | [读写](doc/starrocks.md) 基于 starrocks-spark-connector                              |
| redis         |           | √            | [写](doc/redis.md)                                                                |
| aerospike     | √         | √            | [读写](doc/aerospike.md) 相比redis 性能更好                                              |
| maxcompute    | √         | √            | [读写](doc/maxcompute.md)                                            |

## example
```sql
-- support cte
WITH tmp_demo_test2 AS (SELECT * FROM bigdata.test_demo_test2 where name is not null)
datatunnel SOURCE('hive') OPTIONS(
    databaseName='bigdata',
    tableName='tmp_demo_test2',
    columns=['*'])
SINK('log') OPTIONS(numRows = 10)

-- mysql to hive
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

## 参考

1. [Bucket4j 限流库](https://github.com/vladimir-bukhtoyarov/bucket4j)
2. https://github.com/housepower/spark-clickhouse-connector
3. https://github.com/apache/incubator-seatunnel
4. https://www.oudeis.co/blog/2020/spark-jdbc-throttling-writes/
5. https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/
