package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.plugin.doris.DorisDataTunnelSinkOption
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

object DorisUtils extends Logging {

  /** delta insert select 操作
    */
  def writeStreamSelectAdapter(
      spark: SparkSession,
      checkpointLocation: String,
      triggerProcessingTime: Long,
      sinkOption: DorisDataTunnelSinkOption,
      querySql: String
  ): Unit = {
    val streamingInput = spark.sql(querySql)

    val writer = streamingInput.writeStream
      .options(sinkOption.getProperties)

    val fullTableId = sinkOption.getDatabaseName + "." + sinkOption.getTableName();
    writer
      .trigger(Trigger.ProcessingTime(triggerProcessingTime, TimeUnit.SECONDS))
      .format("doris")
      .options(sinkOption.getProperties())
      .option("doris.fenodes", sinkOption.getFeEnpoints())
      .option("user", sinkOption.getUsername())
      .option("password", sinkOption.getPassword())
      .option("doris.table.identifier", fullTableId)
      .option("checkpointLocation", checkpointLocation)

    if (sinkOption.isPassthrough) {
      writer
        .option("doris.sink.streaming.passthrough", "true")
        .option("doris.sink.properties.format", sinkOption.getFileFormat.name().toLowerCase)
    }

    writer
      .start()
      .awaitTermination()
  }
}
