### Reader

#### 参数说明

| 参数key         | 数据类型    | 是否必填  | 默认值    |描述                                  |
| :-----          | :-----    | :-----   | :------  | :------                             |
| databaseName    | string    |          |          | 数据库名                              |
| tableName       | string    | √        |          | 读取数据表的表名称（大小写不敏感）, 可以包含数据库名，如果没有数据库名，数据库默认为当前项目          |
| column          | array     |          | √        | 源表需要读取的字段, 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。如果读取全部字段，"column": ["*"] |
| condition       | string    |          |          | 设置数据过滤条件，不需要添加 where 关键字 |


### Writer

#### 参数说明

| 参数key         | 数据类型   | 是否必填  | 默认值     |描述                                  |
| :-----          | :-----   | :-----   | :------   | :------                             |
| databaseName    | string   |          |           | 数据库名                              |
| tableName       | string   | √        |           | 读取数据表的表名称（大小写不敏感）, 可以包含数据库名，如果没有数据库名，数据库默认为当前项目          |
| partition       | string   |          |           | 如果分区表必须指定具体分区，数组类型值，可以指定多个分区，partition="ds=20210816,ds=20210817", 多个值逗号分隔         |
| column          | array     |          | √         | 目的表需要写入数据的字段, 字段之间用英文逗号分隔，例如: "column": ["id","name","age"]。如果读取全部字段，"column": ["*"] |
| writeMode       | string   | √        | overwrite | 可选值: append & overwrite |
