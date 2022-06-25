package com.superior.datatunnel.plugin.kafka.reader

import com.superior.datatunnel.api.DataTunnelException
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Created by libinsong on 2020/7/29 12:06 下午
 */
object KafkaSupport {

  def createStreamTempTable(tableName: String, options: util.Map[String, String]) {
    checkKafkaStatus(options)
    options.put("includeHeaders", "true")
    val lineRow = createDataSet(options)
    lineRow.createOrReplaceTempView(tableName);
  }

  private def checkKafkaStatus(options: util.Map[String, String]): Unit = {
    val servers = options.get("kafka.bootstrap.servers")
    val subscribe = options.get("subscribe")

    val props = new Properties()
    props.put("bootstrap.servers", servers)
    props.put("connections.max.idle.ms", "10000")
    props.put("request.timeout.ms", "5000")

    var adminClient: AdminClient = null
    try {
      adminClient = AdminClient.create(props)
      val topics = adminClient.listTopics().namesToListings().get()

      val subscribes = StringUtils.split(subscribe, ",")
      subscribes.foreach(item => {
        if (!topics.containsKey(item)) {
          val value = adminClient.listTopics().names().get().asScala.mkString(",")
          throw new DataTunnelException("topic 不存在: " + item + ", 可用topic: " + value)
        }
      })
    } catch {
      case e: Exception => throw new DataTunnelException("kafka broker " + servers + " 不可用: " + e.getMessage)
    } finally if (adminClient != null) adminClient.close()
  }

  private def createDataSet(options: util.Map[String, String]): Dataset[Row] = {
    val lines = SparkSession.active.readStream.format("kafka").options(options)
      .option("failOnDataLoss", "false")
      .option("auto.offset.reset", "earliest")
      .load

    lines.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as message",
      "topic as kafka_topic", "timestamp", "unix_millis(timestamp) as kafka_timestamp")
  }
}
