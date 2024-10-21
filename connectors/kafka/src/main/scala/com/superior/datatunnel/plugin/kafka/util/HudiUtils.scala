package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.common.util.FsUtils
import com.superior.datatunnel.plugin.kafka.DatalakeDatatunnelSinkOption
import org.apache.commons.lang3.StringUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.model.{HoodieTableType, WriteOperationType}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hive.{HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.Locale
import java.util.concurrent.TimeUnit

/** 多数据源简单适配
  */
object HudiUtils extends Logging {

  def isHudiTable(identifier: TableIdentifier): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(identifier)
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "hudi"
  }

  /** delta insert select 操作
    */
  def writeStreamSelectAdapter(
      spark: SparkSession,
      identifier: TableIdentifier,
      checkpointLocation: String,
      triggerProcessingTime: Long,
      sinkOption: DatalakeDatatunnelSinkOption,
      querySql: String
  ): Unit = {

    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
    val primaryKey = catalogTable.properties("primaryKey")
    val preCombineField = catalogTable.properties("preCombineField")
    if (StringUtils.isBlank(primaryKey)) {
      throw new DataTunnelException(s"$catalogTable 表属性 primaryKey 为空")
    }
    if (StringUtils.isBlank(preCombineField)) {
      throw new DataTunnelException(s"$catalogTable 表属性 preCombineField 为空")
    }

    val partColumns = catalogTable.partitionColumnNames
    val streamingInput = spark.sql(querySql)
    var writer = streamingInput.writeStream
      .format("org.apache.hudi")
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .option(DataSourceWriteOptions.OPERATION.key, WriteOperationType.UPSERT.value)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, primaryKey)
      .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, "true")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, preCombineField)
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "5")
      .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key, "true")
      .option(DataSourceWriteOptions.ASYNC_CLUSTERING_ENABLE.key, "true")

    if (partColumns.nonEmpty) {
      writer.option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partColumns.mkString(","))
    }
    writer.option(HoodieWriteConfig.TBL_NAME.key, identifier.table)
    writer.outputMode(OutputMode.Append)

    FsUtils.mkDir(spark, checkpointLocation)
    writer.option("checkpointLocation", checkpointLocation)

    if ("org.apache.spark.sql.hive.HiveSessionCatalog".equals(spark.sessionState.catalog.getClass.getName)) {
      writer = writer
        .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key, identifier.table)
        .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key, identifier.database.get)
        .option(HiveSyncConfigHolder.HIVE_SYNC_MODE.key, "HMS")
        .option(HiveSyncConfigHolder.HIVE_SYNC_ENABLED.key, "true")
        .option(HoodieSyncConfig.META_SYNC_ENABLED.key, "false")
        .option(
          HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key,
          classOf[MultiPartKeysValueExtractor].getCanonicalName
        )

      if (partColumns.nonEmpty) {
        writer.option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key, partColumns.mkString(","))
      }
    }

    val pros = catalogTable.properties.filter(entry => StringUtils.startsWith(entry._1, "hoodie."))
    writer.options(pros)
      .options(sinkOption.getProperties)
      .start(catalogTable.location.toString)
      .awaitTermination()
  }
}
