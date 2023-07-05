package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.api.DataTunnelException
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.Locale

/**
 * 多数据源简单适配
 */
object HudiUtils extends Logging{

  private val PARTITION_COL_NAME = "ds,kafka_topic";

  def isHudiTable(tableName: String,
                  database: String): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(database)))
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  private def getHudiTablePrimaryKey(spark: SparkSession,
                                     catalogTable: CatalogTable,
                                     properties: Map[String, String]): String = {

    val qualifiedName = catalogTable.qualifiedName
    val isHudiTable = catalogTable.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"

    if (isHudiTable) {
      // scalastyle:off hadoopconfiguration
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // scalastyle:on hadoopconfiguration
      val tablPath = catalogTable.location.toString
      val metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(tablPath).build()
      val tableType = metaClient.getTableType
      if (tableType == null || HoodieTableType.COPY_ON_WRITE == tableType) {
        logError(s"${qualifiedName} ${tableType} location: $tablPath")
        throw new DataTunnelException(s"${qualifiedName} 是hudi COW类型表，不支持流数据写入，请使用MOR类型表")
      } else {
        val partColumns = catalogTable.partitionColumnNames
        if (partColumns.isEmpty || partColumns.size != 2 || !partColumns.mkString(",").equals(PARTITION_COL_NAME)) {
          throw new DataTunnelException(s"${qualifiedName} 必须是分区表，写分区字段名必须为: $PARTITION_COL_NAME")
        }

        logInfo(s"$qualifiedName table properties: ${properties.mkString(",")}")
        val primaryKey = properties("primaryKey")
        if (StringUtils.isBlank(primaryKey)) {
          throw new DataTunnelException(s"$catalogTable 主键不能为空")
        }

        primaryKey
      }
    } else {
      throw new DataTunnelException(s"${catalogTable.qualifiedName} 不是hudi 类型表，不支持流数据写入")
    }
  }

  /**
   * delta insert select 操作
   */
  def deltaInsertStreamSelectAdapter(spark: SparkSession, databaseName: String, tableName: String, querySql: String): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
    val properties = spark.sessionState.catalog.externalCatalog.getTable(databaseName, tableName).properties
    val primaryKey = getHudiTablePrimaryKey(spark, catalogTable, properties)

    val checkpointLocation = s"/user/dataworks/stream_checkpoint/$databaseName.db/$tableName"
    mkCheckpointDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    var writer = streamingInput.writeStream.format("org.apache.hudi")
      .option(DataSourceWriteOptions.OPERATION.key, WriteOperationType.INSERT.value)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, primaryKey)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, PARTITION_COL_NAME)
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "true")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "id")
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "5")
      .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key, "true")
      .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key, "true")
      .option(HoodieWriteConfig.TBL_NAME.key, tableName)
      .option("checkpointLocation", checkpointLocation)
      .outputMode(OutputMode.Append)

    writer = writer.option(DataSourceWriteOptions.HIVE_TABLE.key, tableName)
      .option(DataSourceWriteOptions.HIVE_DATABASE.key, databaseName)
      .option(DataSourceWriteOptions.HIVE_SYNC_MODE.key, "HMS")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED.key, "true")
      .option(DataSourceWriteOptions.META_SYNC_ENABLED.key, "false")

      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key, PARTITION_COL_NAME)
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS.key, classOf[MultiPartKeysValueExtractor].getCanonicalName)

    writer.trigger(Trigger.ProcessingTime(100))
      .start(catalogTable.location.toString)
      .awaitTermination()
  }

  private def mkCheckpointDir(sparkSession: SparkSession, path: String): Unit = {
    val configuration = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(configuration)
    if (!fs.exists(new Path(path))) fs.mkdirs(new Path(path))
  }
}
