## 基于spark 打造的数据集成软件
目前很多公司使用datax 同步数据，存在如下问题：
1. hive 表不支持复杂数据类型(array, map, struct)读写，seatunnel 也有类似问题。基于spark 去实现，对数据类型以及数据格式支持，非常成熟
2. hive 表数据格式支持有限，不支持parquet，数据湖iceberg, hudi, paimon 等。
3. hive 同步直接读取数据文件，不能获取分区信息，不能把分区信息同步到 sink 表。
4. 需要自己管理datax 任务资源，例如：同步任务数量比较多，怎么控制同时运行任务数量，同一个节点运行太多，可能导致CPU 使用过高，触发运维监控。
5. 如果是出海用户，使用redshift、snowflake、bigquery 产品(国产很多数仓缺少大数据引擎connector，例如hashdata，gauss dws)，基于datax api 方式去实现，效率非常低。redshift，snowflake 提供spark connector。底层提供基于copy form/into 命令批量导入数据，效率高。
6. JDBC 缺少bulk insert，效率不高

## 发布打包

#### 独立集成spark 打包
```
-- antlr4 版本要与spark 中版本一致
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Pcdh6
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Phadoop3
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Phadoop2
```

#### superior 平台打包，排除一些平台已经有的jar
```
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Pcdh6
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Phadoop3
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Phadoop2
```

### 构建Spark 镜像
```
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/w6m0k7l2

./bin/docker-image-tool.sh -r public.ecr.aws/w6m0k7l2 -t spark-3.4.2 build
./bin/docker-image-tool.sh -r public.ecr.aws/w6m0k7l2 -t spark-3.4.2 push

docker build -t spark-3.4.2-datatunnel .
docker tag spark-3.4.2-datatunnel public.ecr.aws/w6m0k7l2/spark:spark-3.4.2-datatunnel
docker push public.ecr.aws/w6m0k7l2/spark:spark-3.4.2-datatunnel

./bin/spark-submit \
    --master k8s://https://xxxx.yl4.us-east-1.eks.amazonaws.com \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=public.ecr.aws/w6m0k7l2/spark:spark-3.4.2 \
    --conf spark.kubernetes.file.upload.path=s3a://superior2023/kubenetes \
	--conf spark.hadoop.fs.s3a.access.key=xxx \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  \
	--conf spark.hadoop.fs.s3a.fast.upload=true  \
	--conf spark.hadoop.fs.s3a.secret.key=xxx  \
    s3a://superior2023/spark/spark-examples_2.12-3.4.2.jar 5
```

### 构建AWS EMR镜像
```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 480976988805.dkr.ecr.us-east-1.amazonaws.com
docker logout public.ecr.aws

docker buildx build --platform linux/amd64 -f Dockerfile-EMR -t emr6.15-serverless-spark .
docker tag emr6.15-serverless-spark:latest 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
docker push 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
```


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

| 数据源           | Reader(读) | Writer(写)    | 文档                                                                                |
|:--------------|:----------| :------      |:----------------------------------------------------------------------------------|
| file          | √         | √            | [读写](doc/file.md) 支持excel, json，csv, parquet、orc、text 文件                          |
| sftp          | √         | √            | [读写](doc/sftp.md) 支持excel, json，csv, parquet、orc、text 文件                          |                       
| ftp           | √         | √            | [读写](doc/ftp.md)  支持excel, json，csv, parquet、orc、text 文件                          |
| s3            | √         | √            | [读写](doc/s3.md)  支持excel, json，csv, parquet、orc、text 文件                           |
| hdfs          | √         |              | [读](doc/hdfs.md) 支持excel, json，csv, parquet、orc、text 文件                           |
| jdbc          | √         | √            | [读写](doc/jdbc.md) 支持: mysql，oracle，db2，sqlserver，hana，guass，postgresql            |
| hive          | √         | √            | [读写](doc/hive.md)                                                                 |
| clickhouse    | √         | √            | [读写](doc/clickhouse.md) 基于 spark-clickhouse-connector 项目                          |
| cassandra     | √         | √            | [读写](doc/cassandra.md)                                                            |
| elasticsearch |           | √            | [读写](doc/elasticsearch.md) elasticsearch 7 版本                                     |
| log           |           | √            | [写](doc/log.md)                                                                   |
| doris         | √         | √            | [读写](doc/doris.md) 基于 doris-spark-connector                                       |
| starrocks     | √         | √            | [读写](doc/starrocks.md) 基于 starrocks-spark-connector                               |
| redis         |           | √            | [写](doc/redis.md)                                                                 |
| aerospike     | √         | √            | [读写](doc/aerospike.md) 相比redis 性能更好                                               |
| maxcompute    | √         | √            | [读写](doc/maxcompute.md)                                                           |
| redshift      | √         | √            | [读写](doc/redshift.md)  https://github.com/spark-redshift-community/spark-redshift          |
| snowflake     | √         | √            | [读写](doc/snowflake.md)  https://github.com/snowflakedb/spark-snowflake                     |
| Bigquery      | √         | √            | [读写](doc/bigquery.md)  https://github.com/GoogleCloudDataproc/spark-bigquery-connector     |

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

-- mysql to hive，数据过滤处理
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

-- hive to mysql，字段映射
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
    
-- maxcompute 同步 hive
DATATUNNEL SOURCE("maxcompute") OPTIONS (
    projectName = "datac_test2",
    tableName = "my_table_struct",
    accessKeyId = 'xxx',
    secretAccessKey = 'xxxxx',
    endpoint='http://service.cn-hangzhou.maxcompute.aliyun.com/api',
    columns = ["*"]
)
SINK("hive") OPTIONS (
  databaseName = "default",
  tableName = 'my_table_struct',
  writeMode = 'overwrite',
  partitionSpec = 'pt=20231102',
  columns = ["*"]
)
```

## Spark DistCp 语法 (计划中)

s3、hdfs、ftp、sftp、ftps 之间直接传输文件

```sql
distCp sourcePath options(键值对参数) 
TO sinkPath options(键值对参数)
```


## 参考

1. [Bucket4j 限流库](https://github.com/vladimir-bukhtoyarov/bucket4j)
2. https://github.com/housepower/spark-clickhouse-connector
3. https://github.com/apache/incubator-seatunnel
4. https://www.oudeis.co/blog/2020/spark-jdbc-throttling-writes/
5. https://sparkbyexamples.com/spark/add-multiple-jars-to-spark-submit-classpath/
6. https://github.com/CoxAutomotiveDataSolutions/spark-distcp
7. https://gitlab.com/lwaldmann/Hadoop-FTP-FTPS-SFTP-filesystem
