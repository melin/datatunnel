package org.apache.spark.sql.datatunnel.sql

import com.superior.datatunnel.common.util.CommonUtils
import io.github.melin.jobserver.spark.api.LogUtils
import io.github.melin.superior.common.SQLParserException
import io.github.melin.superior.common.relational.io.ExportTable
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import java.net.URI
import java.util
import scala.collection.JavaConverters._

case class ExportTableCommand(
    exportData: ExportTable,
    nameAndQuery: Seq[(String, String)]
) extends LeafRunnableCommand {

  private final val logger = LoggerFactory.getLogger(classOf[ExportTableCommand])

  private val exportFileName = exportData.getPath
  private val options = exportData.getProperties

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val distPath = genDistPath(sparkSession)
    //cte 语法支持
    val dataFrame = cteQueryAction(sparkSession)

    if (StringUtils.endsWithIgnoreCase(exportFileName, ".json")) {
      exportJson(dataFrame, distPath)
    } else if (StringUtils.endsWithIgnoreCase(exportFileName, ".xlsx")) {
      exportExcel(dataFrame, distPath)
    } else if (StringUtils.endsWithIgnoreCase(exportFileName, ".csv")) {
      exportCsv(dataFrame, distPath)
    } else if (StringUtils.endsWithIgnoreCase(exportFileName, ".txt")) {
      exportText(dataFrame, distPath)
    } else {
      throw new SQLParserException("仅支持导出文件格式: json、xlsx、txt、csv")
    }

    logInfo("导出文件完成: " + distPath)
    LogUtils.info("导出文件完成: " + distPath)
    Seq.empty[Row]
  }

  private def genDistPath(sparkSession: SparkSession): String = {
    val uri = new URI(exportFileName)
    if (uri.getScheme != null || StringUtils.startsWith(exportFileName, "/")) {
      exportFileName
    } else {
      val userId = sparkSession.conf.get("spark.jobserver.superior.userId")
      "/user/superior/users/" + userId + "/export/" + exportFileName
    }
  }

  private def exportExcel(df: DataFrame, distPath: String): Unit = {
    val dataFrame: DataFrame = df

    val overwrite = if (options.containsKey("overwrite")) {
      options.get("overwrite").toBoolean
    } else false

    logger.info("export excel file in " + distPath)
    if (!overwrite) {
      dataFrame.write.format("com.crealytics.spark.excel")
        .option("dataAddress", "MyTable[#All]")
        .option("useHeader", "true")
        .mode(SaveMode.Append)
        .save(distPath)
    } else {
      dataFrame.write.format("com.crealytics.spark.excel")
        .option("dataAddress", "MyTable[#All]")
        .option("useHeader", "true")
        .mode(SaveMode.Overwrite)
        .save(distPath)
    }
  }

  private def exportJson(df: DataFrame, distPath: String): Unit = {
    var dataFrame: DataFrame = df

    val fileCount = if (options.containsKey("fileCount")) {
      options.get("fileCount").toInt
    } else 0

    if (fileCount > 0) {
      dataFrame = dataFrame.coalesce(fileCount)
    }

    var writer = dataFrame.write.format("json")
    val overwrite = if (options.containsKey("overwrite")) {
      options.get("overwrite").toBoolean
    } else true

    for (entry <- options.entrySet().asScala) {
      if (!StringUtils.equalsIgnoreCase(entry.getKey, "fileCount")) {
        writer = writer.option(entry.getKey, entry.getValue)
      }
    }

    writer.save(distPath)
    logger.info("export json file in " + distPath)
    if (!overwrite) {
      writer.option("inferSchema", "true").mode(SaveMode.Append).save(distPath)
    } else {
      writer.option("inferSchema", "true").mode(SaveMode.Overwrite).save(distPath)
    }
  }

  private def exportCsv(df: DataFrame, distPath: String): Unit = {
    var dataFrame: DataFrame = df

    val fileCount = if (options.containsKey("fileCount")) {
      options.get("fileCount").toInt
    } else 0
    val complexTypeToJson = if (options.containsKey("complexTypeToJson")) {
      options.get("complexTypeToJson").toBoolean
    } else false

    if (fileCount > 0) {
      dataFrame = dataFrame.coalesce(fileCount)
    }

    if (complexTypeToJson) {
      val complexTypeNames = new util.ArrayList[String]()
      dataFrame.schema.fields.foreach(field => {
        field.dataType match {
          case _: ArrayType | _: MapType | _: StructType => complexTypeNames.add(field.name)
          case _ =>
        }
      })
      logger.info("complex type :" + complexTypeNames.asScala.mkString(","))
      complexTypeNames.asScala.foreach(name => {
        val col = dataFrame.col(name)
        dataFrame = dataFrame.withColumn(name + "_json", to_json(col))
        dataFrame = dataFrame.drop(name)
      })
    }

    var writer = dataFrame.write.format("csv")
    val overwrite = if (options.containsKey("overwrite")) {
      options.get("overwrite").toBoolean
    } else true

    for (entry <- options.entrySet().asScala) {
      if (!StringUtils.equalsIgnoreCase(entry.getKey, "header")
        && !StringUtils.equalsIgnoreCase(entry.getKey, "inferSchema")
        && !StringUtils.equalsIgnoreCase(entry.getKey, "fileCount")) {
        writer = writer.option(entry.getKey, entry.getValue)
      }
    }

    logger.info("export csv file in " + distPath)
    if (!overwrite) {
      writer.option("header", "true").option("inferSchema", "true").mode(SaveMode.Append).save(distPath)
    } else {
      writer.option("header", "true").option("inferSchema", "true").mode(SaveMode.Overwrite).save(distPath)
    }
  }

  private def exportText(df: DataFrame, distPath: String): Unit = {
    var dataFrame: DataFrame = df

    val fileCount = if (options.containsKey("fileCount")) {
      options.get("fileCount").toInt
    } else 0

    if (fileCount > 0) {
      dataFrame = dataFrame.coalesce(fileCount)
    }

    var writer = dataFrame.write
    val overwrite = if (options.containsKey("overwrite")) {
      options.get("overwrite").toBoolean
    } else true

    for (entry <- options.entrySet().asScala) {
      if (!StringUtils.equalsIgnoreCase(entry.getKey, "header")
        && !StringUtils.equalsIgnoreCase(entry.getKey, "inferSchema")
        && !StringUtils.equalsIgnoreCase(entry.getKey, "fileCount")) {
        writer = writer.option(entry.getKey, entry.getValue)
      }
    }

    logger.info("export csv file in " + distPath)
    if (!overwrite) {
      writer.option("header", "true").option("inferSchema", "true").mode(SaveMode.Append).text(distPath)
    } else {
      writer.option("header", "true").option("inferSchema", "true").mode(SaveMode.Overwrite).text(distPath)
    }
  }

  private def getPartitionCondition(catalogTable: CatalogTable): String = {
    if (catalogTable.partitionColumnNames.isEmpty && exportData.getPartitionVals.size() > 0) {
      throw new SQLParserException("非分区表，不用指定分区")
    }

    if (catalogTable.partitionColumnNames.nonEmpty && exportData.getPartitionVals.size() == 0) {
      throw new SQLParserException("分区表导出请指定具体分区")
    }

    if (catalogTable.partitionColumnNames.nonEmpty) {
      val map = new util.HashMap[String, String]()
      for (partition <- exportData.getPartitionVals.entrySet().asScala) {
        val key = partition.getKey
        val value = partition.getValue
        if (catalogTable.partitionColumnNames.contains(key)) {
          map.put(key, value)
        } else {
          throw new SQLParserException(s"当前表没有分区字段: $key, 分区字段为: " + catalogTable.partitionColumnNames.mkString(","))
        }
      }

      if (map.keySet().size() != catalogTable.partitionColumnNames.size) {
        throw new SQLParserException("当前表分区字段为: " + catalogTable.partitionColumnNames.mkString(","))
      }

      exportData.getPartitionVals.asScala.map { case (key, value) => key + " = " + value }.mkString(" and ")
    } else {
      null
    }
  }

  private def cteQueryAction(sparkSession: SparkSession): DataFrame = {
    val catalog = sparkSession.sessionState.catalog
    val tableName = exportData.getTableId.getTableName
    val db = CommonUtils.getCurrentDatabase(exportData.getTableId.getSchemaName)
    var dataFrame: DataFrame = null
    val dbTable = TableIdentifier(tableName, Option(db))
    // 如果有 cte 语法，先注册临时表
    if (nameAndQuery.nonEmpty) {
      nameAndQuery.foreach {
        case (name, query) =>
          sparkSession.sql(query).createOrReplaceTempView(name)
      }
      dataFrame = sparkSession.table(tableName)
    } else if (catalog.tableExists(dbTable)) {
      val catalogTable = catalog.getTableMetadata(dbTable)
      logger.info("table {} type {}, location: {}", tableName, catalogTable.tableType, catalogTable.location.getPath)

      val condition = getPartitionCondition(catalogTable)
      val sql = if (StringUtils.isBlank(condition)) {
        s"select * from ${db}.${tableName}"
      } else {
        s"select * from ${db}.${tableName} where $condition"
      }

      logger.info("export sql: " + sql)
      dataFrame = sparkSession.sql(sql)
    } else if (catalog.getTempView(tableName).isDefined) {
      val logicPlan = catalog.getTempView(tableName).get
      val execution = sparkSession.sessionState.executePlan(logicPlan)
      execution.assertAnalyzed()

      val clazz = classOf[Dataset[Row]]
      val constructor = clazz.getDeclaredConstructor(classOf[SparkSession], classOf[LogicalPlan], classOf[Encoder[_]])
      dataFrame = constructor.newInstance(sparkSession, logicPlan, RowEncoder.apply(execution.analyzed.schema))
    } else {
      throw new SQLParserException(String.format("table %s is neither exists in database %s nor in the temp view!", tableName, db))
    }

    dataFrame
  }
}
