package com.superior.datatunnel.plugin.kafka.util

import com.superior.datatunnel.plugin.starrocks.StarrocksDataTunnelSinkOption
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

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
    throw new RuntimeException("not support")
  }
}
