package com.superior.datatunnel.core

import com.google.common.collect.Maps
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerJobStart,
  SparkListenerTaskEnd
}
import org.apache.spark.sql.SparkSessionExtensions

import java.util.concurrent.ConcurrentMap
import scala.collection.JavaConverters._

/** huaixin 2021/12/27 2:47 PM
  */
class DataTunnelExtensions()
    extends (SparkSessionExtensions => Unit)
    with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      session.sparkContext.addSparkListener(new SparkListener() {
        private var lastInputRecords = 0L
        private var lastOutputRecords = 0L

        override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
          DataTunnelMetrics.resetMetrics()
        }

        override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
          val metrics = taskEnd.taskMetrics
          if (metrics == null) {
            return
          }

          val enabled =
            session.conf.get("spark.datatunnel.metrics.enabled", "true")
          if (!"true".equals(enabled)) {
            return
          }

          if (metrics.inputMetrics != null) {
            DataTunnelMetrics.inputTaskRecords
              .put(taskEnd.taskInfo.taskId, metrics.inputMetrics.recordsRead)
          }
          if (metrics.outputMetrics != null) {
            DataTunnelMetrics.outputTaskRecords.put(
              taskEnd.taskInfo.taskId,
              metrics.outputMetrics.recordsWritten
            )
          }

          val inputRecords: Long = DataTunnelMetrics.inputRecords()
          val outputRecords: Long = DataTunnelMetrics.outputRecords()

          var msg = ""
          if (inputRecords > 0 && lastInputRecords != inputRecords) {
            msg = s"datatunnel read records: ${inputRecords}."
            lastInputRecords = inputRecords
            logInfo(msg)
          }
          if (outputRecords > 0 && lastOutputRecords != outputRecords) {
            msg = s"datatunnel write records: ${outputRecords}."
            lastOutputRecords = outputRecords
            logInfo(msg)
          }
        }
      })
      new DataTunnelSqlParser(session, parser)
    }
  }
}

object DataTunnelMetrics {
  val inputTaskRecords: ConcurrentMap[Long, Long] = Maps.newConcurrentMap()
  val outputTaskRecords: ConcurrentMap[Long, Long] = Maps.newConcurrentMap()

  def inputRecords(): Long = {
    inputTaskRecords.values().asScala.sum
  }

  def outputRecords(): Long = {
    outputTaskRecords.values().asScala.sum
  }

  def resetMetrics(): Unit = {
    inputTaskRecords.clear()
    outputTaskRecords.clear()
  }
}
