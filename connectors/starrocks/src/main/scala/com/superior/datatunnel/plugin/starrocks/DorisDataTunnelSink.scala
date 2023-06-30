package com.superior.datatunnel.plugin.starrocks

import com.superior.datatunnel.api.model.DataTunnelSinkOption
import com.superior.datatunnel.api.{DataTunnelContext, DataTunnelSink}
import org.apache.spark.sql.{Dataset, Row}

/**
 * huaixin 2021/12/7 8:12 PM
 */
class DorisDataTunnelSink extends DataTunnelSink {

  override def sink(dataset: Dataset[Row], context: DataTunnelContext): Unit = {

  }

  override def getOptionClass: Class[_ <: DataTunnelSinkOption] = classOf[StarrocksDataTunnelSinkOption]
}
