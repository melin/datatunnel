
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

```sql
DATATUNNEL SOURCE("redshift") OPTIONS (
    username = "admin",
    password = "Admin2024",
    jdbcUrl = "jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev",
    schemaName = "public",
    tableName = "users1",
    tempdir = "s3a://datacyber/redshift_temp/",
    region = "us-east-1",
    accessKeyId = "${accessKeyId}",
    secretAccessKey = "${secretAccessKey}",
    iamRole = "${iamRole}",
    columns = ["*"],
    sourceTempView='tdl_users'
)
TRANSFORM = 'select id, userid, age from tdl_users'
SINK ("s3") OPTIONS (
    format = "csv",
    path = "s3a://datacyber/melin1204/20240419",
    writeMode = "overwrite",
    region = "us-east-1",
    fileCount = 2
) 
```

### Writer

#### 参数说明

| 参数key            | 数据类型    | 是否必填    | 默认值    | 描述                                                                                            |
|:-----------------|:--------|:--------|:-------|:----------------------------------------------------------------------------------------------|
| url              | string  | √       |        | A JDBC URL, of the format, jdbc:redshift://host:port/database?user=username&password=password |
| username         | string  | √       |        | The Redshift username. Must be used in tandem with password option.                           |
| password         | string  | √       |        | The Redshift password. Must be used in tandem with user option.                               |
| schemaName       | string  |         |        |                                                                                               |
| tableName        | string  |         |        |                                                                                               |
| query            | string  |         |        | The query to read from in Redshift， query 和 scheama+table二选一                                  |
| region           | string  | √       |        | The AWS region (e.g., 'us-east-1') where your secret resides.                                 |
| tempdir          | string  | √       |        |                                                                                               |
| accessKeyId      | string  | √       |        |                                                                                               |
| secretAccessKey  | string  | √       |        |                                                                                               |
| iamRole          | string  | √       |        | Fully specified ARN of the IAM Role attached to the Redshift cluster,                         |
| writeMode        | string  | √       | append | 支持: append、upsert、 overwrite                                                                  |
| upsertKeyColumns | array[string] |   |        | upsert 写入数据，指定表主键                                                                             |
| preActions       | array[string] |   |        | 写入之前执行的reshift sql，支持多个sql，例如：写入之前删除数据                                                        |
| postActions      | array[string] |   |        | 写入成功后执行的reshift sql，支持多个sql，例如：写入之前删除数据                                                       |
| properties.*     | string  | √       |        | spark redshift connector 更多参数：https://spark.apache.org/docs/latest/sql-data-sources.html      |

```sql
DATATUNNEL SOURCE("s3") OPTIONS (
    format = "json",
    paths = ["s3a://datacyber/melin1204/"],
    region = "us-east-1",
    sourceTempView='tdl_users'
) 
TRANSFORM = 'select id, userid, age from tdl_users'
SINK("redshift") OPTIONS (
    username = "admin",
    password = "Admin2024",
    jdbcUrl = "jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev",
    schemaName = "public",
    tableName = "users1",
    writeMode = "UPSERT",
    upsertKeyColumns = ["id"],
    tempdir = "s3a://datacyber/redshift_temp/",
    region = "us-east-1",
    accessKeyId = "${accessKeyId}",
    secretAccessKey = "${secretAccessKey}",
    iamRole = "${iamRole}",
    columns = ["*"]
) 
```