package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.common.enums.OutputMode
import com.superior.datatunnel.common.util.FsUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util.Locale
import java.util.concurrent.TimeUnit

/** 多数据源简单适配
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
      outputMode: OutputMode,
      querySql: String
  ): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)

    FsUtils.mkDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("delta")
      .outputMode(outputMode.getName)
      .option("checkpointLocation", checkpointLocation)
      .start(catalogTable.location.toString)
      .awaitTermination()
  }
}
