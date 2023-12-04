package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.api.DataTunnelException
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.streaming.OutputMode

import java.util.Locale

/**
 * 多数据源简单适配
 */
object HudiUtils extends Logging{

  private val PARTITION_COL_NAME = "ds";

  def isHudiTable(tableName: String,
                  database: String): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(database)))
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  private def getPrimaryKeyAndPreCombineField(spark: SparkSession,
                                     catalogTable: CatalogTable,
                                     properties: Map[String, String]): (String, String) = {

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
        if (partColumns.isEmpty || partColumns.size != 1 || !partColumns.head.equals(PARTITION_COL_NAME)) {
          throw new DataTunnelException(s"${qualifiedName} 必须是分区表，写分区字段名必须为: $PARTITION_COL_NAME")
        }

        logInfo(s"$qualifiedName table properties: ${properties.mkString(",")}")
        val primaryKey = properties("primaryKey")
        val preCombineField = properties("preCombineField")
        if (StringUtils.isBlank(primaryKey)) {
          throw new DataTunnelException(s"$catalogTable 表属性 primaryKey 为空")
        }
        if (StringUtils.isBlank(preCombineField)) {
          throw new DataTunnelException(s"$catalogTable 表属性 preCombineField 为空")
        }

        (primaryKey, preCombineField)
      }
    } else {
      throw new DataTunnelException(s"${catalogTable.qualifiedName} 不是hudi 类型表，不支持流数据写入")
    }
  }

  /**
   * delta insert select 操作
   */
  def deltaInsertStreamSelectAdapter(
      spark: SparkSession,
      databaseName: String,
      tableName: String,
      checkpointLocation: String,
      querySql: String): Unit = {

    val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(databaseName)))
    val properties = spark.sessionState.catalog.externalCatalog.getTable(databaseName, tableName).properties
    val (primaryKey, preCombineField)  = getPrimaryKeyAndPreCombineField(spark, catalogTable, properties)

    mkCheckpointDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    var writer = streamingInput.writeStream.format("org.apache.hudi")
      .option(DataSourceWriteOptions.OPERATION.key, WriteOperationType.INSERT.value)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, primaryKey)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, PARTITION_COL_NAME)
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "true")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, preCombineField)
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "5")
      .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key, "true")
      .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key, "true")
      .option(HoodieWriteConfig.TBL_NAME.key, tableName)
      .option("checkpointLocation", checkpointLocation)
      .outputMode(OutputMode.Append)

    writer = writer.option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key, tableName)
      .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key, databaseName)
      .option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, "HMS")
      .option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, "true")
      .option(HoodieSyncConfig.META_SYNC_ENABLED.key, "false")

      .option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key, PARTITION_COL_NAME)
      .option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key, classOf[MultiPartKeysValueExtractor].getCanonicalName)

    writer.start(catalogTable.location.toString)
      .awaitTermination()
  }

  private def mkCheckpointDir(sparkSession: SparkSession, path: String): Unit = {
    val configuration = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(configuration)
    if (!fs.exists(new Path(path))) fs.mkdirs(new Path(path))
  }
}
