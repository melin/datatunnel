package com.superior.datatunnel.core

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions


/**
 * huaixin 2021/12/27 2:47 PM
 */
class DataTunnelExtensions() extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new DataTunnelSqlParser(session, parser)
    }
  }
}
