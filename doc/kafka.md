### Reader

> kafka 数据只能写入hive hudi 存储格式表
> 输出hive 只需要填写参数 databseName 和 tableName

```sql
-- kafka 不做消息解析，建表语句如下，
create table kafka_log_dt ( 
    id string comment "默认为kafka key，如果key为空，值为timestamp",
    message string comment "采集消息",
    ds string comment "小时分区：yyyyMMddHH")
using hudi  
primary key (id) with MOR 
PARTITIONED BY (ds)
lifeCycle 100
comment 'hudi demo'

create table test_hudi_demo (
    id string comment "默认为kafka key，如果key为空，值为timestamp",
    message string comment "采集消息",
    ds string comment "小时分区：yyyyMMddHH")
using hudi    
OPTIONS (
   primaryKey='id',
   type='MOR',
   hoodie.parquet.compression.codec='snappy',
   hoodie.payload.event.time.field='ds',
   hoodie.payload.ordering.field='ds',
   hoodie.datasource.write.precombine.field='ds',
   hoodie.metadata.enable=true,
   hoodie.cleaner.commits.retained=24
)
PARTITIONED BY (ds)
comment ‘hudi demo’
```

#### 参数说明

| 参数key    | 数据类型    | 是否必填  | 默认值    | 描述                                                                         |
|:---------|:--------| :-----   | :------  |:---------------------------------------------------------------------------|
| subscribe    | string  |          | 10       | topic                                                                      |
| bootstrap.servers | strint  |          | 20       | kafka 服务器地址                                                                |

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

 