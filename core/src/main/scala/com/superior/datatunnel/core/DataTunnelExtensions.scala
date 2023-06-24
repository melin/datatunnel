package com.superior.datatunnel.core

import io.github.melin.jobserver.spark.api.LogUtils
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSessionExtensions

import java.util.concurrent.atomic.AtomicLong

/**
 * huaixin 2021/12/27 2:47 PM
 */
class DataTunnelExtensions() extends (SparkSessionExtensions => Unit) with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>

      session.sparkContext.addSparkListener(new SparkListener() {
        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          val metrics = taskEnd.taskMetrics
          if (metrics.inputMetrics != None) {
            DataTunnelMetrics.inputRecords.addAndGet(metrics.inputMetrics.recordsRead)
          }
          if (metrics.outputMetrics != None) {
            DataTunnelMetrics.outputRecords.addAndGet(metrics.outputMetrics.recordsWritten)
          }

          if (DataTunnelMetrics.logEnabled) {
            logInfo(s"datatunnel read records: ${DataTunnelMetrics.inputRecords}," +
              s"write records: ${DataTunnelMetrics.outputRecords}")

            LogUtils.info("datatunnel read records: {}, write records: {}",
              DataTunnelMetrics.inputRecords,
              DataTunnelMetrics.outputRecords)
          }
        }
      })
      new DataTunnelSqlParser(session, parser)
    }
  }
}

object DataTunnelMetrics {
  var logEnabled = false;

  val inputRecords = new AtomicLong(0)
  val outputRecords = new AtomicLong(0)

  def resetMetrics(): Unit = {
    inputRecords.set(0)
    outputRecords.set(0)
  }
}
