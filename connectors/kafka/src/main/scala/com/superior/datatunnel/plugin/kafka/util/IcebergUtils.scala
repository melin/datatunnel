package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.common.enums.OutputMode
import com.superior.datatunnel.common.util.FsUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

/** 多数据源简单适配
  */
object IcebergUtils extends Logging {

  private val PARTITION_COL_NAME = "ds";

  def isIcebergTable(identifier: TableIdentifier): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(identifier)
    val tableType = table.properties.get("table_type")
    tableType.isDefined && tableType.get.equalsIgnoreCase("iceberg")
  }

  /** delta insert select 操作
    */
  def writeStreamSelectAdapter(
                                spark: SparkSession,
                                identifier: TableIdentifier,
                                checkpointLocation: String,
                                triggerProcessingTime: Long,
                                outputMode: OutputMode,
                                getMergeKeys: String,
                                querySql: String
                              ): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)

    FsUtils.mkDir(spark, checkpointLocation)

    val streamingInput = spark.sql(querySql)
    val writer = streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("delta")
      .outputMode(outputMode.getName)
      .option("checkpointLocation", checkpointLocation)

    if (StringUtils.isBlank(getMergeKeys)) {
      writer
        .start(catalogTable.location.toString)
        .awaitTermination()
    } else {
      val foreachBatchFn = new ForeachBatchFn(getMergeKeys, identifier)
      writer
        .foreachBatch(foreachBatchFn)
        .outputMode("update")
        .start()
        .awaitTermination()
    }
  }
}
