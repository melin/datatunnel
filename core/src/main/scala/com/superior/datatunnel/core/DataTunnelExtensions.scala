package com.superior.datatunnel.core

import com.google.common.collect.Maps
import io.github.melin.jobserver.spark.api.LogUtils
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSessionExtensions

import java.util.concurrent.ConcurrentMap

import scala.collection.JavaConverters._

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
            DataTunnelMetrics.inputTaskRecords.put(taskEnd.taskInfo.taskId, metrics.inputMetrics.recordsRead)
          }
          if (metrics.outputMetrics != None) {
            DataTunnelMetrics.outputTaskRecords.put(taskEnd.taskInfo.taskId, metrics.outputMetrics.recordsWritten)
          }

          if (DataTunnelMetrics.logEnabled) {
            logInfo(s"datatunnel read records: ${DataTunnelMetrics.inputRecords}," +
              s"write records: ${DataTunnelMetrics.outputRecords}")

            LogUtils.info("datatunnel read records: {}, write records: {}",
              DataTunnelMetrics.inputRecords(),
              DataTunnelMetrics.outputRecords())
          }
        }
      })
      new DataTunnelSqlParser(session, parser)
    }
  }
}

object DataTunnelMetrics {
  var logEnabled = false;

  val inputTaskRecords: ConcurrentMap[Long, Long] = Maps.newConcurrentMap()
  val outputTaskRecords: ConcurrentMap[Long, Long] = Maps.newConcurrentMap()

  def inputRecords(): Long = {
    inputTaskRecords.keySet().asScala.sum
  }

  def outputRecords(): Long = {
    outputTaskRecords.keySet().asScala.sum
  }

  def resetMetrics(): Unit = {
    inputTaskRecords.clear()
    outputTaskRecords.clear()
  }
}
