package com.dataworks.datatunnel.elasticsearch

import com.dataworks.datatunnel.api.{DataTunnelException, DataTunnelSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.elasticsearch.spark.sql._
import scala.collection.JavaConverters._

import java.util

/**
 * huaixin 2022/2/10 11:40 AM
 */
class ElasticsearchWriter extends DataTunnelSink {

  override def validateOptions(options: util.Map[String, String]): Unit = {
    val hosts = options.get("es.hosts")
    if (StringUtils.isBlank(hosts)) {
      throw new DataTunnelException("es.hosts 不能为空")
    }
  }

  override def write(sparkSession: SparkSession, dataset: Dataset[Row], options: util.Map[String, String]): Unit = {
    val index = options.get("es.resource")
    val esCfg = options.asScala.filter(item => StringUtils.startsWith(item._1, "es."))
    dataset.saveToEs(index, esCfg)
  }
}
