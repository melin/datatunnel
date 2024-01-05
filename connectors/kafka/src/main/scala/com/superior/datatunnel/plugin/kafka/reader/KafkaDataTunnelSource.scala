package com.superior.datatunnel.plugin.kafka.reader

import com.gitee.melin.bee.util.SqlUtils
import com.superior.datatunnel.api.DataSourceType.ORACLE
import com.superior.datatunnel.api.model.DataTunnelSourceOption
import com.superior.datatunnel.api.{DataSourceType, DataTunnelContext, DataTunnelException, DataTunnelSource}
import com.superior.datatunnel.common.enums.WriteMode
import com.superior.datatunnel.common.util.JdbcUtils.execute
import com.superior.datatunnel.common.util.{CommonUtils, JdbcUtils}
import com.superior.datatunnel.plugin.hive.HiveDataTunnelSinkOption
import com.superior.datatunnel.plugin.jdbc.JdbcDataTunnelSinkOption
import com.superior.datatunnel.plugin.kafka.{KafkaDataTunnelSinkOption, KafkaDataTunnelSourceOption}
import com.superior.datatunnel.plugin.kafka.util.HudiUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.sql.Connection
import java.util
import scala.concurrent.duration.DurationInt
import scala.collection.JavaConverters._

/**
 * huaixin 2021/12/29 2:23 PM
 */
class KafkaDataTunnelSource extends DataTunnelSource with Logging {

  override def read(context: DataTunnelContext): Dataset[Row] = {
    val tmpTable = "tdl_datatunnel_kafka_" + System.currentTimeMillis()
    val sourceOption = context.getSourceOption.asInstanceOf[KafkaDataTunnelSourceOption]
    KafkaSupport.createStreamTempTable(tmpTable, sourceOption)

    val sinkType = context.getSinkOption.getDataSourceType
    if (DataSourceType.HIVE == sinkType) {
      writeHive(context, sourceOption, tmpTable)
    } else if (DataSourceType.isJdbcDataSource(sinkType)) {
      writeJdbc(context, sourceOption, tmpTable)
    } else if (DataSourceType.LOG == sinkType) {
      writeLog(context, sourceOption, tmpTable)
    } else if (DataSourceType.KAFKA == sinkType) {
      writeKafka(context, sourceOption, tmpTable)
    } else {
      throw new UnsupportedOperationException("kafka 数据不支持同步到 " + sinkType)
    }

    null
  }

  private def writeLog(context: DataTunnelContext, sourceOption: KafkaDataTunnelSourceOption, tmpTable: String): Unit = {
    val querySql = buildQuerySql(context, sourceOption, tmpTable)
    val dataset = context.getSparkSession.sql(querySql)

    val query = dataset.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()
  }

  private def writeKafka(context: DataTunnelContext, sourceOption: KafkaDataTunnelSourceOption, tmpTable: String): Unit = {
    val querySql = buildQuerySql(context, sourceOption, tmpTable)
    val dataset = context.getSparkSession.sql(querySql)
    val kafkaSinkOption = context.getSinkOption.asInstanceOf[KafkaDataTunnelSinkOption]

    val query = dataset.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaSinkOption.getServers)
      .option("topic", kafkaSinkOption.getTopic)
      .option("checkpointLocation", sourceOption.getCheckpointLocation)
      .start()
    query.awaitTermination()
  }

  private def writeHive(context: DataTunnelContext, sourceOption: KafkaDataTunnelSourceOption, tmpTable: String): Unit = {
    val sparkSession = context.getSparkSession
    val hiveSinkOption = context.getSinkOption.asInstanceOf[HiveDataTunnelSinkOption]
    val sinkDatabaseName = hiveSinkOption.getDatabaseName
    val sinkTableName = hiveSinkOption.getTableName
    val checkpointLocation = sourceOption.getCheckpointLocation

    if (!HudiUtils.isHudiTable(sinkTableName, sinkDatabaseName)) {
      throw new DataTunnelException(s"${sinkDatabaseName}.${sinkTableName} 不是hudi类型表")
    }
    val querySql = buildQuerySql(context, sourceOption, tmpTable)
    HudiUtils.deltaInsertStreamSelectAdapter(sparkSession, sinkDatabaseName, sinkTableName, checkpointLocation, querySql)
  }

  def writeJdbc(context: DataTunnelContext, sourceOption: KafkaDataTunnelSourceOption, tmpTable: String): Unit = {
    val sparkSession = context.getSparkSession
    var connection: Connection = null
    try {
      val querySql = buildQuerySql(context, sourceOption, tmpTable)
      var dataset = sparkSession.sql(querySql)
      val tdlName = "tdl_datatunnel_" + System.currentTimeMillis
      dataset.createTempView(tdlName)

      val jdbcSinkOption = context.getSinkOption.asInstanceOf[JdbcDataTunnelSinkOption]
      val dataSourceType = jdbcSinkOption.getDataSourceType
      val sinkDatabaseName = jdbcSinkOption.getDatabaseName
      val sinkTableName = jdbcSinkOption.getTableName
      val table = sinkDatabaseName + "." + sinkTableName

      var jdbcUrl = jdbcSinkOption.getJdbcUrl
      if (StringUtils.isBlank(jdbcUrl)) {
        if (dataSourceType eq ORACLE) throw new DataTunnelException("orcale 数据源请指定 jdbcUrl")

        jdbcUrl = JdbcUtils.buildJdbcUrl(dataSourceType, jdbcSinkOption.getHost,
          jdbcSinkOption.getPort, jdbcSinkOption.getDatabaseName)
      }

      val batchsize = jdbcSinkOption.getBatchsize
      val queryTimeout = jdbcSinkOption.getQueryTimeout

      val writeMode = jdbcSinkOption.getWriteMode
      var mode = SaveMode.Append
      if (WriteMode.OVERWRITE == writeMode) mode = SaveMode.Overwrite

      val truncate = jdbcSinkOption.isTruncate

      val sql = CommonUtils.genOutputSql(dataset, sourceOption.getColumns, jdbcSinkOption.getColumns, jdbcSinkOption.getDataSourceType)
      dataset = sparkSession.sql(sql)

      val preactions = jdbcSinkOption.getPreActions
      if (StringUtils.isNotBlank(preactions)) {
        val options = jdbcSinkOption.getParams
        options.put("user", jdbcSinkOption.getUsername)
        connection = buildConnection(jdbcUrl, table, options)
      }

      if (StringUtils.isNotBlank(preactions)) {
        val sqls = SqlUtils.splitMultiSql(preactions)
        for (presql <- sqls.asScala) {
          logInfo("exec pre sql: " + presql)
          execute(connection, presql)
        }
      }

      val checkpointLocation = sourceOption.getCheckpointLocation
      val query = dataset.writeStream
        .trigger(Trigger.ProcessingTime(1.seconds))
        .outputMode(OutputMode.Update)
        .option("checkpointLocation", checkpointLocation)
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.write
            .format("jdbc")
            .mode(mode)
            .option("url", jdbcUrl)
            .option("dbtable", table)
            .option("batchsize", batchsize)
            .option("queryTimeout", queryTimeout)
            .option("truncate", truncate)
            .option("user", jdbcSinkOption.getUsername)
            .option("password", jdbcSinkOption.getPassword)
            .save
        }.start()

      query.awaitTermination()
    } finally {
      JdbcUtils.close(connection)
    }
  }

  private def buildQuerySql(context: DataTunnelContext, sourceOption: KafkaDataTunnelSourceOption, tmpTable: String): String = {
    val sql = "select * from " + tmpTable

    val transfromSql = context.getTransfromSql
    if (StringUtils.isNotBlank(transfromSql)) {
      val df = context.getSparkSession.sql(sql)
      df.createTempView(sourceOption.getResultTableName)
      transfromSql
    } else {
      sql
    }
  }

  override def getOptionClass: Class[_ <: DataTunnelSourceOption] = classOf[KafkaDataTunnelSourceOption]

  private def buildConnection(url: String, dbtable: String, params: util.Map[String, String]): Connection = {
    val options = new JDBCOptions(url, dbtable, params.asScala.toMap)
    val dialect = JdbcDialects.get(url)
    dialect.createConnectionFactory(options)(-1)
  }

  private def mkCheckpointDir(sparkSession: SparkSession, path: String): Unit = {
    val configuration = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(configuration)
    if (!fs.exists(new Path(path))) fs.mkdirs(new Path(path))
  }
}
