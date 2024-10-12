package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.common.enums.OutputMode
import com.superior.datatunnel.common.util.FsUtils
import com.superior.datatunnel.plugin.kafka.DatalakeDatatunnelSinkOption
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util.Locale
import java.util.concurrent.TimeUnit

/** 多数据源简单适配 https://delta.io/blog/write-kafka-stream-to-delta-lake/
  * https://delta.io/blog/delta-lake-vs-parquet-comparison/ https://delta.io/blog/pros-cons-hive-style-partionining/
  * https://delta.io/blog/2023-01-25-delta-lake-small-file-compaction-optimize/
  */
object DeltaUtils extends Logging {

  private val PARTITION_COL_NAME = "ds";

  def isDeltaTable(identifier: TableIdentifier): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(identifier)
    table.provider.map(_.toLowerCase(Locale.ROOT)).orNull == "delta"
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

    spark.sessionState.catalog.externalCatalog.getTable("bigdata", "delta_users_kafka")
    FsUtils.mkDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    val writer = streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("delta")
      .option("checkpointLocation", checkpointLocation)

    writer.options(sinkOption.getProperties)

    var mergeKeys = sinkOption.getMergeKeys
    val outputMode = sinkOption.getOutputMode
    val partitionColumnNames = sinkOption.getPartitionColumnNames

    if (StringUtils.isBlank(mergeKeys)) {
      if (StringUtils.isNotBlank(partitionColumnNames)) {
        writer.partitionBy(StringUtils.split(partitionColumnNames, ","): _*)
      }

      writer
        .outputMode(outputMode.getName)
        .start(catalogTable.location.toString)
        .awaitTermination()
    } else {
      if (StringUtils.isNotBlank(partitionColumnNames)) {
        mergeKeys = mergeKeys + "," + partitionColumnNames
      }
      val foreachBatchFn = new ForeachBatchFn(mergeKeys, identifier)
      writer
        .foreachBatch(foreachBatchFn)
        .outputMode("update")
        .start()
        .awaitTermination()
    }
  }
}
