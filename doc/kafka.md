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

 