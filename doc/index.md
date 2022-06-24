Datax 两个不同数据源之间交换同步数据，具体语法：

```sql
datatunnel reader('数据类型名称') options(键值对参数) writer('数据类型名称') options(键值对参数)
```

###实例

```sql
-- 从sftp 导入 hive 表
datatunnel source("sftp") options(host="x.x.x.x", port=22, username="sftpuser", password='xxxx',
              path="/upload/demo.csv", fileType="csv", hdfsTempLocation="/user/datawork/temp")
    sink("hive") options(tableName="tdl_ftp_demo")

-- 从hdfs 导入 sftp
datatunnel source("hdfs") options(path="/user/datawork/export/20210812")
    sink("sftp") options(host1="x.x.x.x", port=22, username="sftpuser", password='xxxx',
              path="/upload/20210812", overwrite=true);
```

### 支持数据源

| 数据源           | Reader(读)  | Writer(写)    | 文档                 |
|:--------------| :-----      | :------      |:-------------------|
| sftp          | √           | √            | [读写](sftp.md)      |
| hdfs          | √           |              | [读](hdfs.md)       |
| jdbc          | √           | √            | [读写](jdbc.md)      |
| hive          | √           | √            | [读写](hive.md)      |
| hbase         |             | √            | [写](hbase.md)      |
| clickhouse    |             | √            | [写](clickhouse.md) |
| elasticsearch |             | √            | 开发中                |
| kafka         |             | √            | [写](kafka.md)      |
| log           |             | √            | [写](log.md)        |

