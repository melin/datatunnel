package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.common.util.FsUtils
import com.superior.datatunnel.plugin.kafka.DatalakeDatatunnelSinkOption
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util.Locale
import java.util.concurrent.TimeUnit

/** 多数据源简单适配 https://delta.io/blog/write-kafka-stream-to-delta-lake/
  * https://delta.io/blog/delta-lake-vs-parquet-comparison/ https://delta.io/blog/pros-cons-hive-style-partionining/
  * https://delta.io/blog/2023-01-25-delta-lake-small-file-compaction-optimize/
  */
object DeltaUtils extends Logging {

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
    FsUtils.mkDir(spark, checkpointLocation)

    val outputMode = sinkOption.getOutputMode
    val streamingInput = spark.sql(querySql)
    val writer = streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("delta")
      .option("checkpointLocation", checkpointLocation)
      .outputMode(outputMode.getName)
      .options(sinkOption.getProperties)

    var mergeColumns = sinkOption.getMergeColumns
    val partitionColumnNames = sinkOption.getPartitionColumnNames

    if (StringUtils.isBlank(mergeColumns)) {
      if (StringUtils.isNotBlank(partitionColumnNames)) {
        writer.partitionBy(StringUtils.split(partitionColumnNames, ","): _*)
      }

    } else {
      if (StringUtils.isNotBlank(partitionColumnNames)) {
        mergeColumns = mergeColumns + "," + partitionColumnNames
      }
    }

    val foreachBatchFn = new DeltaForeachBatchFn(mergeColumns, identifier, sinkOption.isCompactionEnabled)
    writer
      .foreachBatch(foreachBatchFn)
      .start()
      .awaitTermination()
  }
}
