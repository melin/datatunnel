package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.common.util.FsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.Locale
import java.util.concurrent.TimeUnit

/** 多数据源简单适配
  */
object HudiUtils extends Logging {

  private val PARTITION_COL_NAME = "ds";

  def isHudiTable(identifier: TableIdentifier): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(identifier)
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  private def getPrimaryKeyAndPreCombineField(
      spark: SparkSession,
      catalogTable: CatalogTable,
      properties: Map[String, String]
  ): (String, String) = {

    val qualifiedName = catalogTable.qualifiedName
    val isHudiTable =
      catalogTable.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"

    if (isHudiTable) {
      // scalastyle:off hadoopconfiguration
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      // scalastyle:on hadoopconfiguration
      val tablPath = catalogTable.location.toString
      val metaClient = HoodieTableMetaClient
        .builder()
        .setConf(new HadoopStorageConfiguration(hadoopConf, true))
        .setBasePath(tablPath)
        .build()
      val tableType = metaClient.getTableType
      if (tableType == null || HoodieTableType.COPY_ON_WRITE == tableType) {
        logError(s"${qualifiedName} ${tableType} location: $tablPath")
        throw new DataTunnelException(
          s"${qualifiedName} 是hudi COW类型表，不支持流数据写入，请使用MOR类型表"
        )
      } else {
        val partColumns = catalogTable.partitionColumnNames
        if (
          partColumns.isEmpty || partColumns.size != 1 || !partColumns.head
            .equals(PARTITION_COL_NAME)
        ) {
          throw new DataTunnelException(
            s"${qualifiedName} 必须是分区表，写分区字段名必须为: $PARTITION_COL_NAME"
          )
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
      throw new DataTunnelException(
        s"${catalogTable.qualifiedName} 不是hudi 类型表，不支持流数据写入"
      )
    }
  }

  /** delta insert select 操作
    */
  def writeStreamSelectAdapter(
      spark: SparkSession,
      identifier: TableIdentifier,
      checkpointLocation: String,
      triggerProcessingTime: Long,
      querySql: String
  ): Unit = {

    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
    val properties = spark.sessionState.catalog.externalCatalog
      .getTable(identifier.database.orNull, identifier.table)
      .properties
    val (primaryKey, preCombineField) =
      getPrimaryKeyAndPreCombineField(spark, catalogTable, properties)

    val streamingInput = spark.sql(querySql)
    var writer = streamingInput.writeStream
      .format("org.apache.hudi")
      .option(DataSourceWriteOptions.OPERATION.key, WriteOperationType.INSERT.value)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, primaryKey)
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, PARTITION_COL_NAME)
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "true")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, preCombineField)
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "5")
      .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key, "true")
      .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key, "true")
      .option(HoodieWriteConfig.TBL_NAME.key, identifier.table)
      .outputMode(OutputMode.Append)

    FsUtils.mkDir(spark, checkpointLocation)
    writer.option("checkpointLocation", checkpointLocation)
    writer.trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))

    writer = writer
      .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key, identifier.table)
      .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key, identifier.database.get)
      .option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, "HMS")
      .option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, "true")
      .option(HoodieSyncConfig.META_SYNC_ENABLED.key, "false")
      .option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key, PARTITION_COL_NAME)
      .option(
        HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key,
        classOf[MultiPartKeysValueExtractor].getCanonicalName
      )

    writer.start(catalogTable.location.toString).awaitTermination()
  }
}
