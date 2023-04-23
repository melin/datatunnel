package com.superior.datatunnel.core

import com.gitee.melin.bee.core.extension.ExtensionLoader
import com.superior.datatunnel.api.{DataSourceType, DataTunnelSink, DataTunnelSource}

object Utils {

  def getDatasourceConnector(sourceType: DataSourceType, sinkType: DataSourceType): (DataTunnelSource, DataTunnelSink) = {

    val readLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSource])
    val writeLoader = ExtensionLoader.getExtensionLoader(classOf[DataTunnelSink])

    val source: DataTunnelSource = readLoader.getExtension(sourceType.name().toLowerCase)
    val sink: DataTunnelSink = writeLoader.getExtension(sinkType.name().toLowerCase)
    (source, sink)
  }
}
