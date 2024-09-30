package com.superior.datatunnel.plugin.kafka

import com.superior.datatunnel.api.model.DataTunnelSinkOption
import com.superior.datatunnel.api.{DataTunnelContext, DataTunnelSink}
import org.apache.spark.sql.{Dataset, Row}

class DataLakeDataTunnelSink extends DataTunnelSink {

  override def sink(dataset: Dataset[Row], context: DataTunnelContext): Unit = {
  }

  override def getOptionClass: Class[_ <: DataTunnelSinkOption] =
    classOf[DatalakeDatatunnelSinkOption]
}
