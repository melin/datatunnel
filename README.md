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

  private val logger = Logger(classOf[DataxExprCommand])

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sourceType = CommonUtils.cleanQuote(ctx.srcName.getText)
    val distType = CommonUtils.cleanQuote(ctx.distName.getText)
    val readOpts = CommonUtils.convertOptions(ctx.readOpts)
    val writeOpts = CommonUtils.convertOptions(ctx.writeOpts)

    getDsConfig(readOpts, writeOpts)

    writeOpts.put("__sourceType__", sourceType)

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataxReader])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataxWriter])

    var reader: DataxReader = null
    var writer: DataxWriter = null
    try {
      reader = readLoader.getExtension(sourceType)
      writer = writeLoader.getExtension(distType)
    } catch {
      case e: IllegalStateException => throw new DataWorkerSQLException(e.getMessage, e)
    }

    reader.validateOptions(readOpts)
    writer.validateOptions(writeOpts)

    val df = reader.read(sparkSession, readOpts)
    writer.write(sparkSession, df, writeOpts)
    Seq.empty[Row]
  }

  private def getDsConfig(readOpts: util.HashMap[String, String], writeOpts: util.HashMap[String, String]): Unit = {
    if (readOpts.containsKey("datasourceCode") || writeOpts.containsKey("datasourceCode")) {
      val threadClazz = Class.forName("com.dataworker.spark.jobserver.driver.util.JdbcTemplateHolder")
      val method = threadClazz.getMethod("getJdbcTemplate")
      val jdbcTemplate = method.invoke(null).asInstanceOf[JdbcTemplate]

      val sql = "select config, type from dc_datax_datasource where code=?";
      if (readOpts.containsKey("datasourceCode")) {
        val datasourceCode = readOpts.get("datasourceCode")
        val map = jdbcTemplate.queryForMap(sql, datasourceCode)
        val config = map.get("config").asInstanceOf[String]
        val dsType = map.get("type").asInstanceOf[String]
        readOpts.put("__dsConf__", config)
        readOpts.put("__dsType__", dsType)
        logger.info("reader datasource code: {}, type: {}, conf: {}", datasourceCode, dsType, config)
      }

      if (writeOpts.containsKey("datasourceCode")) {
        val datasourceCode = writeOpts.get("datasourceCode")
        val map = jdbcTemplate.queryForMap(sql, datasourceCode)
        val config = map.get("config").asInstanceOf[String]
        val dsType = map.get("type").asInstanceOf[String]
        writeOpts.put("__dsConf__", config)
        writeOpts.put("__dsType__", dsType)
        logger.info("writer datasource code: {}, type: {}, conf: {}", datasourceCode, dsType, config)
      }
    }
  }
}
```


