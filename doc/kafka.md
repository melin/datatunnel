### Reader

> kafka 数据支持写入sink: kafka，log, 数据湖(hudi, paimon, iceberg, delta)

#### 参数说明

| 参数key               | 数据类型    | 是否必填  | 默认值  | 描述                                                                                                                                                   |
|:--------------------|:--------| :-----   |:-----|:-----------------------------------------------------------------------------------------------------------------------------------------------------|
| format              | string  |√          |      | 支持：json，text                                                                                                                                         |
| subscribe           | string  |          |      | kafka topic, 多个topic逗号分割                                                                                                                             |
| servers             | string  |          |      | kafka 服务器地址                                                                                                                                          |
| includeHeaders      | boolean |          |      | 输出kafka 消息元数据信息，在transform 可以直接使用元数据字段。包含：kafka_key, kafka_topic, kafka_timestamp, kafka_timestampType, kafka_partition, kafka_offset, kafka_headers |
| columns             | array   |          |      | text：输出value字段，json：需要指定字段类型，格式：[columnName dataType, columnName dataType], 例如：{"id":4,"name":"zhangsan"}，配置值: ['id long', 'name string']            |
| checkpointLocation  | string  |√           |    |                                                                                                                                                      |
| kafkaGroupId        | string  |          |      | 消费组                                                                                                                                                  |
| sourceTempView      | string  |          |      | transform sql 中临时表名                                                                                                                                  |
| properties.*        | string  |          |      | 其它参数参考：https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html                                                                                                                                                     |


> 基于spark streaming kafka 实现，

### Kafka Writer

#### 参数说明

| 参数key   | 数据类型    | 是否必填    | 默认值    | 描述              |
|:--------|:--------|:--------| :------  |:----------------|
| topic   | string  | √       | 10       | topic           |
| servers | strint  | √       | 20       | kafka 服务器地址     |
| properties.*        | string  |         |      | 其它参数参考：https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html |

> key.serializer 和 value.serializer 为 StringSerializer，不可以修改


### DataLake Writer

> 支持数据湖：hudi, paimon, iceberg, delta

#### 参数说明

| 参数key                 | 数据类型   | 是否必填      | 默认值   | 描述                                              |
|:----------------------|:-------|:----------|:------|:------------------------------------------------|
| databaseName          | string | √         | 10    | topic                                           |
| tableName             | strint | √         | 20    | kafka 服务器地址                                     |
| outputMode            | string |          | append | 写入模式, 仅支持：append 和 complete                     |
| outputMode            | string |          | append | 写入模式, 仅支持：append 和 complete                     |
| mergeColumns          | string |          | append | 定义 delta/iceberg merge key，用于 merge sql, 多个值逗号分隔 |
| partitionColumnNames  | string |          | append | 分区字段名, 多个值逗号分隔                                  |
| columns               | array  |          | ["*"] |                                                 |
| properties.*          | string  |         |      | |

> key.serializer 和 value.serializer 为 StringSerializer，不可以修改

### Examples

```sql
--kafka json 格式，写入hudi
DATATUNNEL SOURCE("kafka") OPTIONS (
    format="json",
    subscribe = "users_json",
    servers = "172.18.5.102:9092",
    includeHeaders = true,
    sourceTempView='tdl_users',
    columns = ['id long', 'name string'],
    checkpointLocation = "/user/superior/stream_checkpoint/datatunnel/hudi_users_kafka"
) 
TRANSFORM = "select id, name, date_format(kafka_timestamp, 'yyyMMdd') as ds from tdl_users"
SINK("hudi") OPTIONS (
    databaseName = "bigdata",
    tableName = 'hudi_users_kafka',
    columns = ["*"]
)
     
--kafka json 格式，写入delta 分区表, 按照 mergeColumns 覆盖写入
DATATUNNEL SOURCE("kafka") OPTIONS (
    format="json",
    subscribe = "users_json",
    servers = "172.18.5.46:9092",
    includeHeaders = true,
    sourceTempView='tdl_users',
    columns = ['id long', 'name string'],
    checkpointLocation = "/user/superior/stream_checkpoint/datatunnel/delta_users_kafka"
) 
TRANSFORM = "select id, name, date_format(kafka_timestamp, 'yyyMMdd') as ds from tdl_users"
SINK("delta") OPTIONS (
    databaseName = "bigdata",
    tableName = 'delta_users_kafka',
    mergeColumns = 'id',
    partitionColumnNames = 'ds',
    columns = ["*"]
)
        
--kafka text 格式，写入delta 分区表, kafka value 数据写入 delta 字段 content。
DATATUNNEL SOURCE("kafka") OPTIONS (
    format="text",
    subscribe = "users_json",
    servers = "172.18.5.46:9092",
    includeHeaders = true,
    sourceTempView='tdl_users',
    checkpointLocation = "/user/superior/stream_checkpoint/datatunnel/delta_users_kafka_text"
) 
TRANSFORM = "select value as content, date_format(kafka_timestamp, 'yyyMMdd') as ds from tdl_users"
SINK("delta") OPTIONS (
    databaseName = "bigdata",
    tableName = 'delta_users_kafka_text',
    partitionColumnNames = 'ds',
    columns = ["*"]
)
```

### 参考

1. https://github.com/BenFradet/spark-kafka-writer
2. [如何基于Hudi的Payload机制灵活定制化数据写入方式](https://bbs.huaweicloud.com/blogs/detail/302579)
