### elasticsearchWriter

#### 参数说明

| 参数                       | 数据类型      | 是否必填     | 默认值  | 描述                                     |
|:-------------------------| :-----        |:---------|:-----|:---------------------------------------| 
| es.resource              | string        | √        |      |             |
| es.hosts                 | string        | √        |      | Elasticsearch host, 多个值逗号分割            |
| es.port                  | string        |          | 9200 | Elasticsearch port                                       |
| es.nodes.discovery       | array         |          | true |                                        |
| es.batch.size.bytes      | string        |          | 1mb  |                                        |
| es.batch.size.entries    | string        |          | 1000 |                                        |
| es.mapping.id            | string        |          |      | 主键                                     |

详细配置请参考: https://github.com/elastic/elasticsearch-hadoop/blob/master/mr/src/main/java/org/elasticsearch/hadoop/cfg/ConfigurationOptions.java

 
 
 



 