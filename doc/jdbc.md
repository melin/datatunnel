###JDBC 支持数据库类型

1. DB2
2. MySQL
3. MS Sql
4. Oracle
5. PostgreSQL
6. TIDB
7. Hana
8. Greenplum
9. Gauss

### Reader

#### 参数说明

| 参数key                            | 数据类型       | 是否必填  | 默认值   | 描述                                                                                                                                                                                                                                      |
|:---------------------------------|:-----------| :-----   |:------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| datasourceCode                   | string     | √        |       | 数据源Code                                                                                                                                                                                                                                 |
| databaseName                     | string     | √        |       | 数据库名                                                                                                                                                                                                                                    |
| tableName                        | string     | √        |       | 目的表的表名称, 多个表逗号分割，适用分表场景。                                                                                                                                                                                                                |
| condition                        | string     |          |       | 筛选条件，reader插件根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > time。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句                                                                        |
| column                           | array      |          | √     | 源表需要读取的字段, 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。如果读取全部字段，"column": ["*"]                                                                                                                                                       |
| partitionColumn                  | string     |          |       | 分区字段, 必须是数字、时间类型                                                                                                                                                                                                                        |
| numPartitions                    | int        |          |       | 最大分区数量，必须为整数，当为0或负整数时，实际的分区数为1                                                                                                                                                                                                          |
| pushDownAggregate                | boolean    |          | false |                                                                                                                                                                                                                                         |
| pushDownLimit                    | boolean    |          | false |                                                                                                                                                                                                                                         |
| queryTimeout                     | int        | √        | 0     | The number of seconds the driver will wait for a Statement object to execute to the given number of seconds. Zero means there is no limit. In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout |
| fetchSize                        | int        |          | 1000  | 一次性从数据库中读取多少条数据，MySQL默认一次将所有结果都读取到内存中，在数据量很大时可能会造成OOM，设置这个参数可以控制每次读取fetchSize条数据，而不是默认的把所有数据一次读取出来；开启fetchSize需要满足：数据库版本要高于5.0.2、连接参数useCursorFetch=true。 注意：此参数的值不可设置过大，否则会读取超时，导致任务失败。                                                |
| pushDownPredicate                | boolean    | √        | true  | 该选项用于开启或禁用jdbc数据源的谓词下推。默认是true。如果配置为false，那么所有的filter操作都会由spark来完成。当过滤操作用spark更快时，一般才会关闭下推功能。                                                                                                                                           |


### Writer

#### 参数说明

| 参数key           | 数据类型   | 是否必填  | 默认值    |描述                                  |
| :-----           | :-----    | :-----   | :------  | :------                             |
| datasourceCode   | string    | √        |          | 数据源Code                           |
| databaseName     | string    | √        |          | 数据库名                             |
| tableName        | string    | √        |          | 目的表的表名称                        |
| column           | array     |          | √        | 目的表需要写入数据的字段, 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。如果读取全部字段，"column": ["*"] |
| numPartitions    | int       |          |          | 最大分区数量，必须为整数，当为0或负整数时，实际的分区数为1   |
| queryTimeout     | int       | √        | 0        | The number of seconds the driver will wait for a Statement object to execute to the given number of seconds. Zero means there is no limit. In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout   |
| batchsize        | int       |          | 1000     | The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing.|
| preSql           | string    | √        |          | 写入数据到目的表前，会先执行这里的标准语句                  |
| postSql          | string    | √        |          | 写入数据到目的表后，会执行这里的标准语句                  |
| writeMode        | string    |          | append   | 写入模式: append, overwrite|
| truncate         | boolean   |          | false    | writeMode等于overwrite，truncate=true, 插入之前是否清空表                |

### 参考
1. https://github.com/niutaofan/bazinga
2. https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
3. https://luminousmen.com/post/spark-tips-optimizing-jdbc-data-source-reads

