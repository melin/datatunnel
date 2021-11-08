### 发布打包
```
mvn clean package -DlibScope=provided -Dmaven.test.skip=true
```

### 大数据交换引擎
基于spark sql 扩展, 扩展语法
```sql
--从sftp 导入 hive 表
datax reader("ftp") options(host="x.x.x.x", port=22, username="sftpuser", password='xxxx',
              path="/upload/demo.csv", fileType="csv", hdfsTempLocation="/user/datawork/temp")
    writer("hive") options(tableName="tdl_ftp_demo")

--从hdfs 导入 sftp
datax reader("hdfs") options(path="/user/datawork/export/20210812")
    writer("sftp") options(host1="x.x.x.x", port=22, username="sftpuser", password='xxxx',
              path="/upload/20210812", overwrite=true);
```

```scala
case class DataxExprCommand(ctx: DataxExprContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceType = CommonUtils.cleanQuote(ctx.srcName.getText)
    val distType = CommonUtils.cleanQuote(ctx.distName.getText)
    val readOpts = CommonUtils.convertOptions(ctx.readOpts)
    val writeOpts = CommonUtils.convertOptions(ctx.writeOpts)

    writeOpts.put("__sourceType__", sourceType)

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataxReader])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataxWriter])
    val reader = readLoader.getExtension(sourceType)
    val writer = writeLoader.getExtension(distType)

    reader.validateOptions(readOpts)
    writer.validateOptions(writeOpts)

    val df = reader.read(sparkSession, readOpts)
    writer.write(sparkSession, df, writeOpts)

    Seq.empty[Row]
  }
}
```


