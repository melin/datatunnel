### Reader

#### 参数说明

| 参数key                        | 数据类型    | 是否必填 | 默认值   | 描述                                                                                                                                                               |
|:-----------------------------|:--------|:-----|:------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                         | string  | √    |       | 数据源host                                                                                                                                                          |
| port                         | int     | √    |       | 数据源port                                                                                                                                                          |
| username                     | string  | √    |       | 账号                                                                                                                                                               |
| password                     | string  |      |       | 密码                                                                                                                                                               |
| databaseName                 | string  | √    |       | 数据库名                                                                                                                                                             |
| tableName                    | string  | √    |       | 目的表的表名称, 多个表逗号分割，适用分表场景。                                                                                                                                         |
| condition                    | string  |      |       | 筛选条件，reader插件根据指定的column、table、where条件拼接SQL，并根据这个SQL进行数据抽取。在实际业务场景中，往往会选择当天的数据进行同步，可以将where条件指定为gmt_create > time。注意：不可以将where条件指定为limit 10，limit不是SQL的合法where子句 |
| columns                      | array   | √    | ["*"] | 源表需要读取的字段, 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。如果读取全部字段，"column": ["*"]                                                                                |
| protocol                     | string  | √    | http  | 支持http 和 grpc，默认值 http                                                                                                                                           |
| ignoreUnsupportedTransform   | boolean |      | false |                                                                                                                                                                  |
| compressionCodec             | string  |      | lz4   |                                                                                                                                                                  |
| distributedConvertLocal      | boolean |      | true  |                                                                                                                                                                  |
| format                       | string  |      | json  |                                                                                                                                                                  |
| splitByPartitionId           | boolean |      | true  |                                                                                                                                                                  |


### Writer

#### 参数说明

| 参数key                       | 数据类型    | 是否必填     | 默认值   | 描述                                                                                   |
|:----------------------------|:--------|:---------|:------|:-------------------------------------------------------------------------------------|
| host                        | string  | √        |       | 数据源host                                                                              |
| port                        | int     | √        |       | 数据源port                                                                              |
| username                    | string  | √        |       | 账号                                                                                   |
| password                    | string  |          |       | 密码                                                                                   |
| databaseName                | string  | √        |       | 数据库名                                                                                 |
| tableName                   | string  | √        |       | 目的表的表名称                                                                              |
| columns                     | array   | √        | ["*"] | 目的表需要写入数据的字段, 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。如果读取全部字段，"column": ["*"] |
| protocol                    | string  | √        | http  | 支持http 和 grpc，默认值 http                                                               |
| batchSize                   | int     |          | 10000 |                                                                                      |
| compressionCodec            | string  |          | lz4   |                                                                                      |
| distributedConvertLocal     | boolean |          | false |                                                                                      |
| distributedUseClusterNodes  | boolean |          | true  |                                                                                      |
| format                      | string  |          | arrow |                                                                                      |
| localSortByKey              | boolean |          | true  |                                                                                      |
| localSortByPartition        | boolean |          |       |                                                                                      |
| maxRetry                    | int     |          | 3     |                                                                                      |
| repartitionByPartition      | boolean |          | true  |                                                                                      |
| repartitionNum              | int     |          | 0     |                                                                                      |
| repartitionStrictly         | boolean |          | false |                                                                                      |
| retryInterval               | string  |          | 10s   |                                                                                      |
| retryableErrorCodes         | 241     |          | 241   |                                                                                      |

### 参考
1. https://github.com/housepower/spark-clickhouse-connector
