package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.plugin.starrocks.StarrocksDataTunnelSinkOption
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

object StarrocksUtils extends Logging {

  /** delta insert select 操作
    */
  def writeStreamSelectAdapter(
      spark: SparkSession,
      checkpointLocation: String,
      triggerProcessingTime: Long,
      sinkOption: StarrocksDataTunnelSinkOption,
      querySql: String
  ): Unit = {
    val streamingInput = spark.sql(querySql)

    val writer = streamingInput.writeStream
      .options(sinkOption.getProperties)

    val fullTableId = sinkOption.getDatabaseName + "." + sinkOption.getTableName();
    writer
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("starrocks")
      .options(sinkOption.getProperties())
      .option("starrocks.fenodes", sinkOption.getFeEnpoints())
      .option("starrocks.fe.jdbc.url", sinkOption.getJdbcUrl())
      .option("user", sinkOption.getUsername())
      .option("password", sinkOption.getPassword())
      .option("starrocks.table.identifier", fullTableId)
      .option("starrocks.columns", sinkOption.getColumns.mkString(","))
      .option("checkpointLocation", checkpointLocation)

    writer
      .start()
      .awaitTermination()
  }
}
