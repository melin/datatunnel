## 基于spark 的数据集成平台
目前很多公司使用datax 同步数据，存在如下问题：
1. hive 表不支持复杂数据类型(array, map, struct)读写，seatunnel 也有类似问题。基于spark 去实现，对数据类型以及数据格式支持，非常成熟
2. hive 表数据格式支持有限，不支持parquet，数据湖iceberg, hudi, paimon 等。
3. hive 同步直接读取数据文件，不能获取分区信息，不能把分区信息同步到 sink 表。
4. 需要自己管理datax 任务资源，例如：同步任务数量比较多，怎么控制同时运行任务数量，同一个节点运行太多，可能导致CPU 使用过高，触发运维监控。
5. 如果是出海用户，使用redshift、snowflake、bigquery 产品(国产很多数仓缺少大数据引擎connector，例如hashdata，gauss dws)，基于datax api 方式去实现，效率非常低。redshift，snowflake 提供spark connector。底层提供基于copy form/into 命令批量导入数据，效率高。
6. JDBC 缺少bulk insert，效率不高

## Build
[build.md](build.md)

## 部署

解压 assembly/target/ 目录下生成可用包 datatunnel-[version].tar.gz。复制所有jar 到 spark_home/jars 
在conf/spark-default.conf 添加配置: 
> spark.sql.extensions com.superior.datatunnel.core.DataTunnelExtensions

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
datatunnel help (source | sink | all) ('数据源类型名称')

```

## 支持数据源

JDBC 写入支持Append，Overwrite, Upsert，如果是Postgresql 数据库，支持Copy From 导入数据，Copy From 支持Upsert 能力

| 数据源           | Reader(读) | Writer(写)    | 文档                                                                                                                                                                              |
|:--------------|:----------| :------      |:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| file          | √         | √            | [读写](doc/file.md) 支持excel, json，csv, parquet、orc、text 文件                                                                                                                        |
| sftp          | √         | √            | [读写](doc/sftp.md) 支持excel, json，csv, parquet、orc、text 文件                                                                                                                        |                       
| ftp           | √         | √            | [读写](doc/ftp.md)  支持excel, json，csv, parquet、orc、text 文件                                                                                                                        |
| s3            | √         | √            | [读写](doc/s3.md)  支持excel, json，csv, parquet、orc、text 文件                                                                                                                         |
| hdfs          | √         |              | [读](doc/hdfs.md) 支持excel, json，csv, parquet、orc、text 文件                                                                                                                         |
| jdbc          | √         | √            | [读写](doc/jdbc.md) 支持: mysql，oracle，db2，sqlserver，hana，guass，postgresql                                                                                                          |
| hive          | √         | √            | [读写](doc/hive.md)                                                                                                                                                               |
| hbase         | √         | √            | 读  [hbase spark config](https://github.com/apache/hbase-connectors/blob/master/spark/hbase-spark/src/main/scala/org/apache/hadoop/hbase/spark/datasources/HBaseSparkConf.scala) |
| clickhouse    | √         | √            | [读写](doc/clickhouse.md) 基于 spark-clickhouse-connector 项目                                                                                                                        |
| cassandra     | √         | √            | [读写](doc/cassandra.md)                                                                                                                                                          |
| elasticsearch |           | √            | [读写](doc/elasticsearch.md) elasticsearch 7 版本                                                                                                                                   |
| log           |           | √            | [写](doc/log.md)                                                                                                                                                                 |
| kafka         | √         | √            | [写](doc/kafka.md)                                                                                                                                                               |
| doris         | √         | √            | [读写](doc/doris.md) 基于 doris-spark-connector                                                                                                                                     |
| starrocks     | √         | √            | [读写](doc/starrocks.md) 基于 starrocks-spark-connector                                                                                                                             |
| redis         |           | √            | [写](doc/redis.md)                                                                                                                                                               |
| aerospike     | √         | √            | [读写](doc/aerospike.md) 相比redis 性能更好                                                                                                                                             |
| maxcompute    | √         | √            | [读写](doc/maxcompute.md)                                                                                                                                                         |
| redshift      | √         | √            | [读写](doc/redshift.md)  https://github.com/spark-redshift-community/spark-redshift                                                                                               |
| snowflake     | √         | √            | [读写](doc/snowflake.md)  https://github.com/snowflakedb/spark-snowflake                                                                                                          |
| Bigquery      | √         | √            | [读写](doc/bigquery.md)  https://github.com/GoogleCloudDataproc/spark-bigquery-connector                                                                                          |
| Mongodb       | √         | √            | [读写]                                                                                                                                                                            |
| HDFS          | √         | √            | [读写] 例如 阿里云 rds 数据导出 oss， 再通过discp 写入到本地hdfs                                                                                                                                    |

## [example](examples%2Fsrc%2Fmain%2Fkotlin%2Fcom%2Fsuperior%2Fdatatunnel%2Fexamples)

> 结合平台管理数据源，避免在sql中直接写入数据源账号信息

![Reshift 更新插入 Mysql](doc%2Fimgs%2Fredshift_mysql.png)


## 数据导出: Export

文档: [export.md](doc%2Fexport.md)

```
-- 通过文件名后缀，指定导出文件格式，目前支持：txt、csv、json、excel 三种文件
WITH common_table_expression [ , ... ]
export table tablename [PARTITION (part_column="value"[, ...])] TO 'export_file_name.[txt|csv|json|xlsx]' [options(key=value)]
```

## Spark DistCp 语法
s3、hdfs、ftp、sftp、ftps 之间直接传输文件

```sql
distCp sourcePath options(键值对参数) 
TO sinkPath options(键值对参数)
```
| 参数              | 默认值                  | 描述                     |
|-----------------|----------------------|------------------------|
| srcPaths        |                      | 待同步文件或者目录，支持多值         |
| destPath        |                      | 同步输出目录                 |
| update          | false                | Overwrite if source and destination differ in size, or checksum                       |
| overwrite       | false                | Overwrite destination, |
| delete          | false                | Delete the files existing in the dst but not in src          |
| ignoreErrors    |                      | 执行未做任何更改的试运行           |
| dryrun          |                      | Perform a trial run with no changes made           |
| maxFilesPerTask | 1000                 | Maximum number of files to copy in a single Spark task        |
| maxBytesPerTask | 1073741824L          | Maximum number of bytes to copy in a single Spark task        |
| numListstatusThreads  | 10   | Number of threads to use for building file listing          |
| consistentPathBehaviour   | false | Revert the path behaviour when using overwrite or update to the path behaviour of non-overwrite/non-update          |
| includes          |                      | |
| excludes          |                      | |

```sql
set spark.hadoop.fs.oss.endpoint = oss-cn-hangzhou.aliyuncs.com;
set spark.hadoop.fs.oss.accessKeyId = xxx;
set spark.hadoop.fs.oss.accessKeySecret = xxx;
set spark.hadoop.fs.oss.attempts.maximum = 3;
set spark.hadoop.fs.oss.connection.timeout = 10000;
set spark.hadoop.fs.oss.connection.establish.timeout = 10000;
set spark.hadoop.fs.oss.impl = org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem;
-- mysql 数据导出到 oss
DATATUNNEL SOURCE("mysql") OPTIONS (
  username = "root",
  password = "root2023",
  host = '172.18.5.44',
  port = 3306,
  databaseName = 'demos',
  tableName = 'orders',
  columns = ["*"]
) 
SINK("hdfs") OPTIONS (
  filePath = "oss://melin1204/users",
  writeMode = "overwrite"
)

-- oss 复制到 hdfs
DISTCP OPTIONS (
  srcPaths = ['oss://melin1204/users'],
  destPath = "hdfs://cdh1:8020/temp",
  overwrite = true,
  delete = true,
  excludes = [".*/_SUCCESS"]
)
```

## 参考

1. [Bucket4j 限流库](https://github.com/vladimir-bukhtoyarov/bucket4j)
2. https://github.com/housepower/spark-clickhouse-connector
3. https://github.com/apache/incubator-seatunnel
4. https://www.oudeis.co/blog/2020/spark-jdbc-throttling-writes/
5. https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/
6. https://github.com/CoxAutomotiveDataSolutions/spark-distcp
7. https://gitlab.com/lwaldmann/Hadoop-FTP-FTPS-SFTP-filesystem
