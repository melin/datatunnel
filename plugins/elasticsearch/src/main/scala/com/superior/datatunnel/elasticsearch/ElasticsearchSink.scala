package com.superior.datatunnel.elasticsearch

import com.superior.datatunnel.api.{DataTunnelSinkContext, DataTunnelSink}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row}
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters._

/**
 * huaixin 2022/2/10 11:40 AM
 */
class ElasticsearchSink extends DataTunnelSink[EsSinkOption] {

  override def sink(dataset: Dataset[Row], context: DataTunnelSinkContext[EsSinkOption]): Unit = {
    val sinkOption = context.getSinkOption
    val index = sinkOption.getResource
    val esCfg = sinkOption.getParams.asScala.filter(item => StringUtils.startsWith(item._1, "es."))
    dataset.saveToEs(index, esCfg)
  }
}
