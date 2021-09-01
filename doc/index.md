Datax 两个不同数据源之间交换同步数据，具体语法：

```sql
datax reader('数据类型名称') options(键值对参数) writer('数据类型名称') options(键值对参数)
```

###实例

```sql
--从sftp 导入 hive 表
datax reader("sftp") options(host="x.x.x.x", port=22, username="sftpuser", password='xxxx',
              path="/upload/demo.csv", fileType="csv", hdfsTempLocation="/user/datawork/temp")
    writer("hive") options(tableName="tdl_ftp_demo")

--从hdfs 导入 sftp
datax reader("hdfs") options(path="/user/datawork/export/20210812")
    writer("sftp") options(host1="x.x.x.x", port=22, username="sftpuser", password='xxxx',
              path="/upload/20210812", overwrite=true);
```

### 支持数据源

| 数据源         | Reader(读)  | Writer(写)    |文档                          |
| :-----        | :-----      | :------      | :------                      |
| sftp          | √           | √            | [读写](sftp.html)      |
| hdfs          | √           |              | [读](hdfs.html)        |
| mysql         | √           | √            | [读写](mysql.html)        |
| hive          | √           | √            | [读写](hive.html)        |
| hbase         |             | √            | [写](hbase.html)         |
| clickhouse    |             | √            | 开发中        |
| elasticsearch |             | √            | 开发中        |
| log           |             | √            | [写](log.html)       |

