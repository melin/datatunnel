package com.superior.datatunnel.plugin.kafka.reader

import com.superior.datatunnel.api.DataTunnelException
import com.superior.datatunnel.plugin.kafka.KafkaDataTunnelSourceOption
import io.github.melin.jobserver.spark.api.LogUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** Created by libinsong on 2020/7/29 12:06 下午
  */
object KafkaSupport {

  def createStreamTempTable(
      tableName: String,
      sourceOption: KafkaDataTunnelSourceOption
  ): Unit = {
    checkKafkaStatus(sourceOption)

    val params = new util.HashMap[String, String]
    params.putAll(sourceOption.getProperties)

    params.put("includeHeaders", sourceOption.isIncludeHeaders.toString)
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
      params.put(
        "startingOffsetsByTimestampStrategy",
        sourceOption.getStartingOffsetsByTimestampStrategy
      )
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

    val schemaInfo = lineRow.schema.treeString(Int.MaxValue)
    LogUtils.info("source schema: \n" + schemaInfo)
    lineRow.createOrReplaceTempView(tableName);
  }

  private def checkKafkaStatus(
      sourceOption: KafkaDataTunnelSourceOption
  ): Unit = {
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
          val value =
            adminClient.listTopics().names().get().asScala.mkString(",")
          throw new DataTunnelException(
            "topic 不存在: " + item + ", 可用topic: " + value
          )
        }
      })
    } catch {
      case e: Exception =>
        throw new DataTunnelException(
          "kafka broker " + servers + " 不可用: " + e.getMessage
        )
    } finally if (adminClient != null) adminClient.close()
  }

  private def createDataSet(
      options: util.Map[String, String],
      sourceOption: KafkaDataTunnelSourceOption
  ): Dataset[Row] = {
    var rows = SparkSession.active.readStream
      .format("kafka")
      .options(options)
      .load

    val columns = sourceOption.getColumns
    val format = sourceOption.getFormat
    if ("json" == format) {
      if (columns.length == 1 && "*".equals(columns(0))) {
        throw new IllegalArgumentException("json 格式，需要指定 columns. 例如：['id long', 'name string']")
      } else {
        val jsonSchema = columns.mkString(",")
        rows = rows
          .withColumn("data", from_json(col("value").cast("String"), StructType.fromDDL(jsonSchema)))

        if (sourceOption.isIncludeHeaders) {
          rows = rows.selectExpr(
            "key as kafka_key",
            "topic as kafka_topic",
            "timestamp as kafka_timestamp",
            "timestampType as kafka_timestampType",
            "partition as kafka_partition",
            "offset as kafka_offset",
            "headers as kafka_headers",
            "data.*"
          )
        } else {
          rows = rows.selectExpr("data.*")
        }
      }
    } else {
      if (sourceOption.isIncludeHeaders) {
        rows = rows.selectExpr(
          "key as kafka_key",
          "topic as kafka_topic",
          "timestamp as kafka_timestamp",
          "timestampType as kafka_timestampType",
          "partition as kafka_partition",
          "offset as kafka_offset",
          "headers as kafka_headers",
          "CAST(value AS STRING) as value"
        )
      } else {
        rows = rows.selectExpr("CAST(value AS STRING) as value")
      }
    }
    rows
  }
}
