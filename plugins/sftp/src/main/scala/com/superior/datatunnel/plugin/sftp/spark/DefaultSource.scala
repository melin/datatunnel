package com.superior.datatunnel.plugin.sftp.spark

import com.springml.sftp.client.SFTPClient
import Constants.ImplicitDataFrameWriter
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.io.File
import java.util.UUID

/**
 * Datasource to construct dataframe from a sftp url
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider  {
  @transient val logger = Logger.getLogger(classOf[DefaultSource])

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Copy the file from SFTP to local location and then create dataframe using local file
   */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val inferSchema = parameters.get("inferSchema")
    val header = parameters.getOrElse("header", "true")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val quote = parameters.getOrElse("quote", "\"")
    val encoding = parameters.getOrElse("encoding", "UTF-8")
    val escape = parameters.getOrElse("escape", "\\")
    val multiLine = parameters.getOrElse("multiLine", "false")
    val createDF = parameters.getOrElse("createDF", "true")
    val copyLatest = parameters.getOrElse("copyLatest", "false")
    val tempFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val rowTag = parameters.getOrElse(Constants.xmlRowTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml", "orc")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val inferSchemaFlag = if (inferSchema != null && inferSchema.isDefined) {
      inferSchema.get
    } else {
      "false"
    }

    val instanceCode = sqlContext.sparkContext.getConf.get("spark.datawork.job.code")
    val hdfsTemp = "/user/datawork/temp/sftp/" + instanceCode;
    mkdir(sqlContext, hdfsTemp)

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port,
      cryptoKey, cryptoAlgorithm)
    val copiedFileLocation = copy(sftpClient, path, tempFolder, copyLatest.toBoolean)
    val fileLocation = copyToHdfs(sqlContext, copiedFileLocation, hdfsTemp)

    if (!createDF.toBoolean) {
      logger.info("Returning an empty dataframe after copying files...")
      createReturnRelation(sqlContext, schema)
    } else {
      DatasetRelation(fileLocation, fileType, inferSchemaFlag, header, delimiter,
        quote, escape, encoding, multiLine, rowTag, schema, sqlContext)
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
                              parameters: Map[String, String], data: DataFrame): BaseRelation = {

    val username = parameters.get("username")
    val password = parameters.get("password")
    val pemFileLocation = parameters.get("pem")
    val pemPassphrase = parameters.get("pemPassphrase")
    val host = parameters.getOrElse("host", sys.error("SFTP Host has to be provided using 'host' option"))
    val port = parameters.get("port")
    val path = parameters.getOrElse("path", sys.error("'path' must be specified"))
    val fileType = parameters.getOrElse("fileType", sys.error("File type has to be provided using 'fileType' option"))
    val header = parameters.getOrElse("header", "true")
    val tmpFolder = parameters.getOrElse("tempLocation", System.getProperty("java.io.tmpdir"))
    val cryptoKey = parameters.getOrElse("cryptoKey", null)
    val cryptoAlgorithm = parameters.getOrElse("cryptoAlgorithm", "AES")
    val delimiter = parameters.getOrElse("delimiter", ",")
    val quote = parameters.getOrElse("quote", "\"")
    val escape = parameters.getOrElse("escape", "\\")
    val encoding = parameters.getOrElse("encoding", "UTF-8")
    val multiLine = parameters.getOrElse("multiLine", "false")
    val codec = parameters.getOrElse("codec", null)
    val rowTag = parameters.getOrElse(Constants.xmlRowTag, null)
    val rootTag = parameters.getOrElse(Constants.xmlRootTag, null)

    val supportedFileTypes = List("csv", "json", "avro", "parquet", "txt", "xml", "orc")
    if (!supportedFileTypes.contains(fileType)) {
      sys.error("fileType " + fileType + " not supported. Supported file types are " + supportedFileTypes)
    }

    val instanceCode = sqlContext.sparkContext.getConf.get("spark.datawork.job.code")
    val hdfsTemp = "/user/datawork/temp/sftp/" + instanceCode;
    mkdir(sqlContext, hdfsTemp)

    val sftpClient = getSFTPClient(username, password, pemFileLocation, pemPassphrase, host, port,
      cryptoKey, cryptoAlgorithm)
    val tempFile = writeToTemp(sqlContext, data, hdfsTemp, tmpFolder, fileType, header,
      delimiter, quote, escape, encoding, multiLine, codec, rowTag, rootTag)

    upload(tempFile, path, sftpClient)
    createReturnRelation(data)
  }

  private def copyToHdfs(sqlContext: SQLContext, fileLocation: String, hdfsTemp: String): String = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(fileLocation)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      fs.copyFromLocalFile(new Path(fileLocation), new Path(hdfsTemp))
      val filePath = hdfsTemp + "/" + hdfsPath.getName
      fs.deleteOnExit(new Path(filePath))
      filePath
    } else {
      fileLocation
    }
  }

  private def copyFromHdfs(sqlContext: SQLContext, hdfsTemp: String,
                           fileLocation: String): String = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(hdfsTemp)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    if ("hdfs".equalsIgnoreCase(fs.getScheme)) {
      fs.copyToLocalFile(new Path(hdfsTemp), new Path(fileLocation))
      fs.deleteOnExit(new Path(hdfsTemp))
      fileLocation
    } else {
      hdfsTemp
    }
  }

  private def upload(source: String, target: String, sftpClient: SFTPClient) {
    logger.info("Copying " + source + " to " + target)
    sftpClient.copyToFTP(source, target)
  }

  private def getSFTPClient(
     username: Option[String],
     password: Option[String],
     pemFileLocation: Option[String],
     pemPassphrase: Option[String],
     host: String,
     port: Option[String],
     cryptoKey : String,
     cryptoAlgorithm : String) : SFTPClient = {

    val sftpPort = if (port != null && port.isDefined) {
      port.get.toInt
    } else {
      22
    }

    val cryptoEnabled = cryptoKey != null

    if (cryptoEnabled) {
      new SFTPClient(getValue(pemFileLocation), getValue(pemPassphrase), getValue(username),
        getValue(password),
        host, sftpPort, cryptoEnabled, cryptoKey, cryptoAlgorithm)
    } else {
      new SFTPClient(getValue(pemFileLocation), getValue(pemPassphrase), getValue(username),
        getValue(password), host, sftpPort)
    }
  }

  private def createReturnRelation(data: DataFrame): BaseRelation = {
    createReturnRelation(data.sqlContext, data.schema)
  }

  private def createReturnRelation(sqlContextVar: SQLContext, schemaVar: StructType): BaseRelation = {
    new BaseRelation {
      override def sqlContext: SQLContext = sqlContextVar
      override def schema: StructType = schemaVar
    }
  }

  private def copy(sftpClient: SFTPClient, source: String,
                   tempFolder: String, latest: Boolean): String = {
    var copiedFilePath: String = null
    try {
      val target = tempFolder + File.separator + FilenameUtils.getName(source)
      copiedFilePath = target
      if (latest) {
        copiedFilePath = sftpClient.copyLatest(source, tempFolder)
      } else {
        logger.info("Copying " + source + " to " + target)
        copiedFilePath = sftpClient.copy(source, target)
      }

      copiedFilePath
    } finally {
      addShutdownHook(copiedFilePath)
    }
  }

  private def getValue(param: Option[String]): String = {
    if (param != null && param.isDefined) {
      param.get
    } else {
      null
    }
  }

  private def writeToTemp(sqlContext: SQLContext, df: DataFrame,
                          hdfsTemp: String, tempFolder: String, fileType: String, header: String,
                          delimiter: String, quote: String, escape: String, encoding: String,
                          multiLine: String, codec: String, rowTag: String, rootTag: String): String = {
    val randomSuffix = "spark_sftp_connection_temp_" + UUID.randomUUID
    val hdfsTempLocation = hdfsTemp + File.separator + randomSuffix
    val localTempLocation = tempFolder + File.separator + randomSuffix

    addShutdownHook(localTempLocation)

    fileType match {
      case "xml" => df.coalesce(1).write.format(Constants.xmlClass)
        .option(Constants.xmlRowTag, rowTag)
        .option(Constants.xmlRootTag, rootTag).save(hdfsTempLocation)
      case "csv" => df.coalesce(1).
        write.option("header", header).
        option("delimiter", delimiter).
        option("quote", quote).
        option("escape", escape).
        option("encoding", encoding).
        option("multiLine", multiLine).
        optionNoNull("codec", Option(codec)).
        csv(hdfsTempLocation)
      case "txt" => df.coalesce(1).write.text(hdfsTempLocation)
      case "avro" => df.coalesce(1).write.format("com.databricks.com.dataworker.datax.sftp.spark.avro").save(hdfsTempLocation)
      case _ => df.coalesce(1).write.format(fileType).save(hdfsTempLocation)
    }

    copyFromHdfs(sqlContext, hdfsTempLocation, localTempLocation)
    copiedFile(localTempLocation)
  }

  private def addShutdownHook(tempLocation: String) {
    logger.debug("Adding hook for file " + tempLocation)
    val hook = new DeleteTempFileShutdownHook(tempLocation)
    // scalastyle:off runtimeaddshutdownhook
    Runtime.getRuntime.addShutdownHook(hook)
    // scalastyle:on runtimeaddshutdownhook
  }

  private def copiedFile(tempFileLocation: String): String = {
    val baseTemp = new File(tempFileLocation)
    val files = baseTemp.listFiles().filter { x =>
      (!x.isDirectory()
        && !x.getName.contains("SUCCESS")
        && !x.isHidden()
        && !x.getName.contains(".crc")
        && !x.getName.contains("_committed_")
        && !x.getName.contains("_started_")
        )
    }
    files(0).getAbsolutePath
  }

  private def mkdir(sqlContext: SQLContext, fileLocation: String): Unit = {
    val fileSystem = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    fileSystem.mkdirs(new Path(fileLocation))
  }
}
