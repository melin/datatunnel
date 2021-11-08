### clickWriter

#### 支持的数据类型
| 类型       | 说明    | 
| :-----    | :-----  | 
| String    |   |
| Integer   |   |
| Long      |   |
| Double    |   |
| Short     |   |
| Float     |   |
| Date      | |
| TimeStamp |最后以DateTime存入click |

#### 参数说明

| 参数                    | 数据类型    | 是否必填|  默认值        |描述         |
| :-----                 | :-----  | :------| :------         | :------      | 
| datasourceCode   | string    | √        |          | 数据源Code                           |
| databaseName     | string    | √        |          | 数据库名                             |
| tableName                  | String  | √      |                 |目的表的表名称  |
| column                | List |        | √            |目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": ["id","name","age"]。 |
| numPartitions        | string  |         | 8                  |写入clickhouse的分区数 |
| rewriteBatchedStatements        | string  |         | true                  |  |
| batchsize        | string  |         | 200000                  |批量提交条数 |

 
 
 



 