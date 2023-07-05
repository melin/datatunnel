package com.superior.datatunnel.plugin.kafka.reader

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.plugin.kafka.KafkaDataTunnelSourceOption
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

  def createStreamTempTable(tableName: String, sourceOption: KafkaDataTunnelSourceOption): Unit = {
    checkKafkaStatus(sourceOption)

    val params = new util.HashMap[String, String]
    params.putAll(sourceOption.getProperties)

    params.put("includeHeaders", "true")
    if (StringUtils.isNotBlank(sourceOption.getAssign)) {
      params.put("assign", sourceOption.getAssign())
    }
    if (StringUtils.isNotBlank(sourceOption.getSubscribe)) {
      params.put("subscribe", sourceOption.getSubscribe())
    }
    if (StringUtils.isNotBlank(sourceOption.getSubscribePattern)) {
      params.put("subscribePattern", sourceOption.getSubscribePattern())
    }

    if (StringUtils.isNotBlank(sourceOption.getGroupIdPrefix)) {
      params.put("groupIdPrefix", sourceOption.getSubscribePattern())
    }
    if (StringUtils.isNotBlank(sourceOption.getKafkaGroupId)) {
      params.put("kafka.group.id", sourceOption.getKafkaGroupId)
    }
    if (StringUtils.isNotBlank(sourceOption.getStartingOffsetsByTimestampStrategy)) {
      params.put("startingOffsetsByTimestampStrategy", sourceOption.getStartingOffsetsByTimestampStrategy)
    }
    if (sourceOption.getMinPartitions != null) {
      params.put("minPartitions", sourceOption.getMinPartitions.toString)
    }
    if (StringUtils.isNotBlank(sourceOption.getMaxTriggerDelay)) {
      params.put("maxTriggerDelay", sourceOption.getMaxTriggerDelay)
    }

    params.put("kafka.bootstrap.servers", sourceOption.getServers)
    params.put("startingOffsets", sourceOption.getStartingOffsets)
    params.put("failOnDataLoss", sourceOption.isFailOnDataLoss.toString)
    val lineRow = createDataSet(params, sourceOption)
    lineRow.createOrReplaceTempView(tableName);
  }

  private def checkKafkaStatus(sourceOption: KafkaDataTunnelSourceOption): Unit = {
    val servers = sourceOption.getServers
    val subscribe = sourceOption.getSubscribe

    if (StringUtils.isBlank(subscribe)) {
      return
    }

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

  private def createDataSet(options: util.Map[String, String],
                            sourceOption: KafkaDataTunnelSourceOption): Dataset[Row] = {
    val lines = SparkSession.active.readStream.format("kafka")
      .options(options).load

    if (sourceOption.isIncludeHeaders) {
      lines.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as message",
        "topic as kafka_topic", "timestamp", "unix_millis(timestamp) as kafka_timestamp")
    } else {
      lines.selectExpr("CAST(value AS STRING) as message")
    }
  }
}
