package com.superior.datatunnel.plugin.doris

import com.superior.datatunnel.api.model.DataTunnelSourceOption
import com.superior.datatunnel.api.{DataTunnelContext, DataTunnelSource}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

/**
 * huaixin 2021/12/29 2:23 PM
 */
class DorisDataTunnelSource extends DataTunnelSource with Logging {

  override def read(context: DataTunnelContext): Dataset[Row] = {
    null
  }

  override def getOptionClass: Class[_ <: DataTunnelSourceOption] = classOf[DorisDataTunnelSourceOption]
}
