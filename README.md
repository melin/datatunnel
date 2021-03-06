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
    transform("action 名称") options(键值对参数)
    sink('数据类型名称') options(键值对参数)
```

### example
```sql
datatunnel source('mysql') options(
    username='dataworks',
    password='dataworks2021',
    host='10.5.20.20",
    port=3306,
    resultTableName='temp_dc_job',
    databaseName='dataworks', tableName='dc_datax_datasource', column=["*"])
    transform = 'select * from temp_dc_job where type="spark_sql"'
    sink('hive') options(databaseName='bigdata', tableName='hive_datax_datasource', writeMode='overwrite', column=["*"]);

datatunnel source("hive") options(
        databaseName="bigdata", 
        tableName='hive_datax_datasource', 
        column=['id', 'code', 'type', 'description', 'config', 'gmt_created', 'gmt_modified', 'creater', 'modifier'])
    sink('mysql') options(
        username='dataworks',
        password='dataworks2021',
        host="10.5.20.20',
        port=3306
        databaseName='dataworks', 
        tableName='dc_datax_datasource_copy1', 
        writeMode='overwrite',
        truncate=true,
        column=['id', 'code', 'dstype', 'description', 'config', 'gmt_created', 'gmt_modified', 'creater', 'modifier'])
```

### 参考

1. [Bucket4j 限流库](https://github.com/vladimir-bukhtoyarov/bucket4j)
2. https://github.com/housepower/spark-clickhouse-connector
3. https://github.com/apache/incubator-seatunnel
