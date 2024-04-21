
### Reader
https://github.com/spark-redshift-community/spark-redshift?tab=readme-ov-file#parameters

#### 参数说明
| 参数key         | 数据类型  | 是否必填  | 默认值    |描述        |
| :-----          | :-----   | :-----   | :------  | :------     |
| url             | string   | √        |          | A JDBC URL, of the format, jdbc:redshift://host:port/database?user=username&password=password  |
| username        | string   | √        |          | The Redshift username. Must be used in tandem with password option.            |
| password        | string   | √         |          | The Redshift password. Must be used in tandem with user option.            |
| schemaName      | string   |         |          |            |
| tableName       | string   |         |          |            |
| query           | string   |          |          | The query to read from in Redshift， query 和 scheama+table二选一          |
| region          | string   | √        |          | The AWS region (e.g., 'us-east-1') where your secret resides.           |
| tempdir         | string   | √        |          |            |
| accessKeyId     | string   | √        |          |            |
| secretAccessKey     | string   | √        |          |            |
| iamRole     | string   | √        |          | Fully specified ARN of the IAM Role attached to the Redshift cluster,         |
| properties.*     | string   | √        |          |spark redshift connector 更多参数：https://spark.apache.org/docs/latest/sql-data-sources.html |


### Writer

#### 参数说明

| 参数key            | 数据类型  | 是否必填  | 默认值    |描述                                  |
| :-----            | :-----   | :-----   | :------  | :------                             |
| path              | string   | √        |          |            |
| username          | string   | √        |          |            |
| password          | string   |          |          |如果指定 keyFilePath 值，优先使用秘钥认证          |
| host              | string   | √        |          |            |
| port              | int      | √        | 22       |            |
| keyFilePath       | string   |          |          |秘钥文件路径，用户上传存在[我的资源]中          |
| passPhrase        | string   |          |          |密钥的密码         |
| overwrite         | boolean  |          | false    |相同文件已经存在，是否覆盖写入。等于true，第一此执行失败，后续重复执行，实现续传的效果|
