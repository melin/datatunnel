### Reader

> kafka 数据只能写入hive hudi 存储格式表
> 输出hive 只需要填写参数 databseName 和 tableName

```sql
-- kafka 不做消息解析，建表语句如下，
create table kafka_log_dt (
    id string comment "默认为kafka key，如果key为空，值为timestamp",
    message string comment "采集消息",
    kafka_timestamp bigint,
    ds string comment "小时分区：yyyyMMddHH",
    kafka_topic string comment "subscribe可以配置多个topic，通过kafka_topic分区消息")
using hudi  
primary key (id) with MOR 
PARTITIONED BY (ds,kafka_topic)
lifeCycle 100
comment 'hudi demo'

--直接bin/spark-sql 建表语句
create table kafka_log_dt (
    id string comment "默认为kafka key，如果key为空，值为timestamp",
    message string comment "采集消息",
    kafka_timestamp bigint,
    ds string comment "小时分区：yyyyMMddHH",
    kafka_topic string comment "subscribe可以配置多个topic，通过kafka_topic分区消息")
using hudi    
OPTIONS (
   primaryKey='id',
   type='mor',
   hoodie.parquet.compression.codec='snappy',
   hoodie.metadata.enable=true,
   hoodie.payload.event.time.field='kafka_timestamp',
   hoodie.payload.ordering.field='kafka_timestamp',
   hoodie.datasource.write.precombine.field='id',
   hoodie.cleaner.commits.retained=24
)
PARTITIONED BY (ds,kafka_topic)
comment 'hudi demo'
```

#### 参数说明

| 参数key    | 数据类型    | 是否必填  | 默认值    | 描述          |
|:---------|:--------| :-----   | :------  |:------------|
| subscribe    | string  |          | 10       | 多个topic逗号分割 |
| bootstrap.servers | strint  |          | 20       | kafka 服务器地址 |

> 基于spark streaming kafka 实现，详细参数请参考：https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

### Writer

#### 参数说明

| 参数key    | 数据类型    | 是否必填  | 默认值    | 描述                                                                         |
|:---------|:--------| :-----   | :------  |:---------------------------------------------------------------------------|
| topic    | string  |          | 10       | topic                                                                      |
| bootstrap.servers | strint  |          | 20       | kafka 服务器地址                                                                |

> 其它参数参考：https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html
> key.serializer 和 value.serializer 参数已经固定，不可以修改


### 参考

1. https://github.com/BenFradet/spark-kafka-writer
2. [如何基于Hudi的Payload机制灵活定制化数据写入方式](https://bbs.huaweicloud.com/blogs/detail/302579)
