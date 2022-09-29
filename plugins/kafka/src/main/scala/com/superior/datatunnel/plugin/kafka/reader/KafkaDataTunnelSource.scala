package com.superior.datatunnel.plugin.kafka.reader

import com.superior.datatunnel.api.model.DataTunnelSourceOption
import com.superior.datatunnel.api.{DataSourceType, DataTunnelContext, DataTunnelException, DataTunnelSource}
import com.superior.datatunnel.common.util.{CommonUtils, JdbcUtils}
import com.superior.datatunnel.plugin.hive.HiveDataTunnelSinkOption
import com.superior.datatunnel.plugin.jdbc.JdbcDataTunnelSinkOption
import com.superior.datatunnel.plugin.kafka.KafkaDataTunnelSourceOption
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
    val sparkSession = context.getSparkSession
    val tmpTable = "tdl_datatunnel_kafka_" + System.currentTimeMillis()
    KafkaSupport.createStreamTempTable(tmpTable, context.getSourceOption.getParams)

    val sinkType = context.getSinkOption.getDataSourceType
    if (DataSourceType.HIVE == sinkType) {
      val hiveSinkOption = context.getSinkOption.asInstanceOf[HiveDataTunnelSinkOption]
      val sinkDatabaseName = hiveSinkOption.getDatabaseName
      val sinkTableName = hiveSinkOption.getTableName

      if (!HudiUtils.isHudiTable(sinkTableName, sinkDatabaseName)) {
        throw new DataTunnelException(s"${sinkDatabaseName}.${sinkTableName} 不是hudi类型表")
      }
      val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
        "message, kafka_timestamp, date_format(timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable
      HudiUtils.deltaInsertStreamSelectAdapter(sparkSession, sinkDatabaseName, sinkTableName, querySql)
    } else if (DataSourceType.isJdbcDataSource(sinkType)) {
      var connection: Connection = null
      try {
        val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
          "message, kafka_timestamp, date_format(timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable

        var dataset = context.getSparkSession.sql(querySql)
        val tdlName = "tdl_datatunnel_" + System.currentTimeMillis
        dataset.createTempView(tdlName)

        val jdbcSinkOption = context.getSinkOption.asInstanceOf[JdbcDataTunnelSinkOption]
        val sinkDatabaseName = jdbcSinkOption.getDatabaseName
        val sinkTableName = jdbcSinkOption.getTableName
        val table = sinkDatabaseName + "." + sinkTableName

        val url = JdbcUtils.buildJdbcUrl(jdbcSinkOption.getDataSourceType, jdbcSinkOption.getHost,
          jdbcSinkOption.getPort, jdbcSinkOption.getSchema)

        var batchsize = jdbcSinkOption.getBatchsize
        var queryTimeout = jdbcSinkOption.getQueryTimeout

        val writeMode = jdbcSinkOption.getWriteMode
        var mode = SaveMode.Append
        if ("overwrite" == writeMode) mode = SaveMode.Overwrite

        var truncate = jdbcSinkOption.isTruncate

        val sql = CommonUtils.genOutputSql(dataset, jdbcSinkOption.getColumns, jdbcSinkOption.getTableName)
        dataset = sparkSession.sql(sql)

        val preSql = jdbcSinkOption.getPreSql
        val postSql = jdbcSinkOption.getPostSql
        if (StringUtils.isNotBlank(preSql) || StringUtils.isNotBlank(postSql)) {
          val options = jdbcSinkOption.getParams
          options.put("user", jdbcSinkOption.getUsername)
          connection = buildConnection(url, table, options)
        }

        if (StringUtils.isNotBlank(preSql)) {
          logInfo("exec preSql: " + preSql)
          JdbcUtils.execute(connection, preSql)
        }

        val checkpointLocation = s"/user/superior/stream_checkpoint/$sinkDatabaseName.db/$sinkTableName"
        mkCheckpointDir(sparkSession, checkpointLocation)
        val query = dataset.writeStream
          .trigger(Trigger.ProcessingTime(1.seconds))
          .outputMode(OutputMode.Update)
          .option("checkpointLocation", "")
          .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
            batchDF.write
              .format("jdbc")
              .mode(mode)
              .option("url", url)
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
    } else {
      throw new UnsupportedOperationException("kafka 数据不支持同步到 " + sinkType)
    }

    null
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
