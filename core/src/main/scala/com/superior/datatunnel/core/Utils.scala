package com.superior.datatunnel.core

import com.gitee.melin.bee.core.extension.ExtensionLoader
import com.superior.datatunnel.api.{DataSourceType, DataTunnelSink, DataTunnelSource, DistCpSink, DistCpSource}

object Utils {

  def getDataTunnelConnector(sourceType: DataSourceType, sinkType: DataSourceType): (DataTunnelSource, DataTunnelSink) = {

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSource])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSink])

    val source: DataTunnelSource = readLoader.getExtension(sourceType.name().toLowerCase)
    val sink: DataTunnelSink = writeLoader.getExtension(sinkType.name().toLowerCase)
    (source, sink)
  }

  def getDistCpConnector(sourceType: DataSourceType, sinkType: DataSourceType): (DistCpSource, DistCpSink) = {

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DistCpSource])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DistCpSink])

    val source: DistCpSource = readLoader.getExtension(sourceType.name().toLowerCase)
    val sink: DistCpSink = writeLoader.getExtension(sinkType.name().toLowerCase)
    (source, sink)
  }
}
