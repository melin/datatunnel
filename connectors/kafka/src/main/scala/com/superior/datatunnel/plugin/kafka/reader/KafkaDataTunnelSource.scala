package com.superior.datatunnel.plugin.kafka.reader

import com.superior.datatunnel.api.model.DataTunnelSourceOption
import com.superior.datatunnel.api.{DataSourceType, DataTunnelContext, DataTunnelException, DataTunnelSource}
import com.superior.datatunnel.plugin.kafka.{KafkaDataTunnelSinkOption, KafkaDataTunnelSourceOption}
import com.superior.datatunnel.plugin.kafka.util.HudiUtils
import com.superior.datatunnel.plugin.hive.HiveDataTunnelSinkOption
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

/** huaixin 2021/12/29 2:23 PM
  */
class KafkaDataTunnelSource extends DataTunnelSource with Logging {

  override def read(context: DataTunnelContext): Dataset[Row] = {
    val tmpTable = "tdl_datatunnel_kafka_" + System.currentTimeMillis()
    val sourceOption =
      context.getSourceOption.asInstanceOf[KafkaDataTunnelSourceOption]
    KafkaSupport.createStreamTempTable(tmpTable, sourceOption)

    val sinkType = context.getSinkOption.getDataSourceType
    if (DataSourceType.HIVE == sinkType) {
      writeHive(context, sourceOption, tmpTable)
    } else if (DataSourceType.LOG == sinkType) {
      writeLog(context, sourceOption, tmpTable)
    } else if (DataSourceType.KAFKA == sinkType) {
      writeKafka(context, sourceOption, tmpTable)
    } else {
      throw new UnsupportedOperationException("kafka 数据不支持同步到 " + sinkType)
    }

    null
  }

  private def writeLog(
      context: DataTunnelContext,
      sourceOption: KafkaDataTunnelSourceOption,
      tmpTable: String
  ): Unit = {
    val querySql = buildQuerySql(context, sourceOption, tmpTable)
    val dataset = context.getSparkSession.sql(querySql)

    val query = dataset.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()
  }

  private def writeKafka(
      context: DataTunnelContext,
      sourceOption: KafkaDataTunnelSourceOption,
      tmpTable: String
  ): Unit = {
    val querySql = buildQuerySql(context, sourceOption, tmpTable)
    val dataset = context.getSparkSession.sql(querySql)
    val kafkaSinkOption =
      context.getSinkOption.asInstanceOf[KafkaDataTunnelSinkOption]

    val query = dataset.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaSinkOption.getServers)
      .option("topic", kafkaSinkOption.getTopic)
      .option("checkpointLocation", sourceOption.getCheckpointLocation)
      .start()
    query.awaitTermination()
  }

  private def writeHive(
      context: DataTunnelContext,
      sourceOption: KafkaDataTunnelSourceOption,
      tmpTable: String
  ): Unit = {
    val sparkSession = context.getSparkSession
    val hiveSinkOption =
      context.getSinkOption.asInstanceOf[HiveDataTunnelSinkOption]
    val sinkDatabaseName = hiveSinkOption.getDatabaseName
    val sinkTableName = hiveSinkOption.getTableName
    val checkpointLocation = sourceOption.getCheckpointLocation

    if (!HudiUtils.isHudiTable(sinkTableName, sinkDatabaseName)) {
      throw new DataTunnelException(
        s"${sinkDatabaseName}.${sinkTableName} 不是hudi类型表"
      )
    }
    val querySql = buildQuerySql(context, sourceOption, tmpTable)
    HudiUtils.deltaInsertStreamSelectAdapter(
      sparkSession,
      sinkDatabaseName,
      sinkTableName,
      checkpointLocation,
      querySql
    )
  }

  private def buildQuerySql(
      context: DataTunnelContext,
      sourceOption: KafkaDataTunnelSourceOption,
      tmpTable: String
  ): String = {
    val sql = "select * from " + tmpTable

    val transfromSql = context.getTransfromSql
    if (StringUtils.isNotBlank(transfromSql)) {
      val df = context.getSparkSession.sql(sql)
      df.createTempView(sourceOption.getSourceTempView)
      transfromSql
    } else {
      sql
    }
  }

  override def getOptionClass: Class[_ <: DataTunnelSourceOption] =
    classOf[KafkaDataTunnelSourceOption]

}
