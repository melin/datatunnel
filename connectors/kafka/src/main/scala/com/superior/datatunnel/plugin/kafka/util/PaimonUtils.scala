package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.plugin.kafka.DatalakeDatatunnelSinkOption
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

/** 多数据源简单适配
  */
object PaimonUtils extends Logging {

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
      sinkOption: DatalakeDatatunnelSinkOption,
      querySql: String
  ): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)

    val streamingInput = spark.sql(querySql)
    val partitionColumnNames = sinkOption.getPartitionColumnNames

    var writer = streamingInput.writeStream
      .options(sinkOption.getProperties)

    if (StringUtils.isNotBlank(partitionColumnNames)) {
      writer = writer.partitionBy(StringUtils.split(partitionColumnNames, ","): _*)
    }

    writer
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("paimon")
      .outputMode(sinkOption.getOutputMode.getName)
      .option("checkpointLocation", checkpointLocation)
      .start(catalogTable.location.toString)
      .awaitTermination()
  }
}
