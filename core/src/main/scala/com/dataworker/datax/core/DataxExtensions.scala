package com.dataworker.datax.core

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

/**
 * huaixin 2021/12/27 2:47 PM
 */
class DataxExtensions() extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new DataxSqlParser(session, parser)
    }
  }
}
