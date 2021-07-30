### 大数据交换引擎
基于spark sql 扩展, 扩展语法
```sql
datax reader("ftp") options(host="10.10.9.11", port=22, username="sftpuser", password='dz@2021',
              path="/upload/demo.csv", fileType="csv", hdfsTempLocation="/user/datawork/temp")
    writer("hive") options(tableName="tdl_ftp_demo")
```

```scala
case class DataxExprCommand(ctx: DataxExprContext) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceType = CommonUtils.cleanQuote(ctx.srcName.getText)
    val distType = CommonUtils.cleanQuote(ctx.distName.getText)
    val readOpts = CommonUtils.convertOptions(ctx.readOpts)
    val writeOpts = CommonUtils.convertOptions(ctx.writeOpts)

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataxReader])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataxWriter])
    val reader = readLoader.getExtension(sourceType)
    val writer = writeLoader.getExtension(distType)

    reader.validateOptions(readOpts)
    reader.validateOptions(readOpts)

    val df = reader.read(sparkSession, readOpts)
    writer.write(sparkSession, df, writeOpts)

    Seq.empty[Row]
  }
}
```



