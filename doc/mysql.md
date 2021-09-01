### Writer

#### 参数说明

| 参数key           | 数据类型   | 是否必填  | 默认值    |描述                                  |
| :-----           | :-----    | :-----   | :------  | :------                             |
| jdbcUrl          | string    | √        |          | jdbcUrl按照Mysql官方规范, 可以添加参数  |
| username         | string    | √        |          | 目的数据库的用户名                     |
| password         | string    | √        |          | 目的数据库的密码                       |
| databaseName     | string    | √        |          | 数据库名                             |
| tableName        | string    | √        |          | 目的表的表名称                        |
| columns          | string    | √        |          | 目的表需要写入数据的字段,字段之间用英文逗号分隔。例如: "column": "id,name,age"                  |
| preSql           | string    | √        |          | 写入数据到目的表前，会先执行这里的标准语句                  |
| postSql          | string    | √        |          | 写入数据到目的表后，会执行这里的标准语句                  |
| writeMode        | string    | √        | insert   | 控制写入数据到目标表采用 insert into 或者 replace into 或者 ON DUPLICATE KEY UPDATE 语句, 所有选项：insert/replace/update                |
| batchSize        | string    | √        | insert   | 一次性批量提交的记录数大小，该值可以极大减少DataX与Mysql的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。  |


