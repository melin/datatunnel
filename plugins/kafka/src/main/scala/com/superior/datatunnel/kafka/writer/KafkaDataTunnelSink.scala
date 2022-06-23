package com.superior.datatunnel.kafka.writer

import com.superior.datatunnel.api.DataTunnelSink
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import scala.collection.JavaConverters._
import scala.util.parsing.json.JSONObject

/**
 * huaixin 2021/12/7 8:12 PM
 */
class KafkaDataTunnelSink extends DataTunnelSink {

  override def validateOptions(options: util.Map[String, String]): Unit = {
    val topic = options.get("topic")
    if (StringUtils.isBlank(topic)) throw new IllegalArgumentException("topic 不能为空")

    val servers = options.get("bootstrap.servers")
    if (StringUtils.isBlank(servers)) throw new IllegalArgumentException("topic不能为空 不能为空")
  }

  override def write(sparkSession: SparkSession, dataset: Dataset[Row], options: util.Map[String, String]): Unit = {
    val topic = options.get("topic")

    options.put("key.serializer", classOf[StringSerializer].getName)
    options.put("value.serializer", classOf[StringSerializer].getName)

    val map = options.asScala.filter{ case (key, _) => !key.startsWith("__") && key != "topic" }
    val config = collection.immutable.Map(map.toSeq: _*)
    dataset.writeToKafka(
      config,
      row => new ProducerRecord[String, String](topic, convertRowToJSON(row))
    )
  }

  def convertRowToJSON(row: Row): String = {
    if (row.schema.fieldNames.length == 1) {
      row.getString(0)
    } else {
      val m = row.getValuesMap(row.schema.fieldNames)
      JSONObject(m).toString()
    }
  }
}