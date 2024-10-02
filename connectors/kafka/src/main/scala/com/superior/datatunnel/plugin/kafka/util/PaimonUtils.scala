package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.common.enums.OutputMode
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

/** 多数据源简单适配
  */
object PaimonUtils extends Logging {

  private val PARTITION_COL_NAME = "ds";

  def isPaimonTable(identifier: TableIdentifier): Boolean = {
    val table = SparkSession.active.sessionState.catalog.getTableMetadata(identifier)
    val tableType = table.properties.get("table_type")
    tableType.isDefined && tableType.get.equalsIgnoreCase("paimon")
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

    val streamingInput = spark.sql(querySql)
    streamingInput.writeStream
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("paimon")
      .outputMode(outputMode.getName)
      .option("checkpointLocation", checkpointLocation)
      .start(catalogTable.location.toString)
      .awaitTermination()
  }
}
