package com.dataworks.datatunnel.kafka.reader

import com.dataworks.datatunnel.api.{DataTunnelException, DataTunnelSource}
import com.dataworks.datatunnel.common.util.{AESUtil, CommonUtils, JdbcUtils}
import com.dataworks.datatunnel.kafka.util.HudiUtils
import com.gitee.melin.bee.util.MapperUtils
import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.sql.Connection
import java.util
import scala.concurrent.duration.DurationInt
import scala.collection.JavaConverters._

/**
 * huaixin 2021/12/29 2:23 PM
 */
class KafkaReader extends DataTunnelSource with Logging {

  override def validateOptions(options: util.Map[String, String]): Unit = {
    val subscribe = options.get("subscribe")
    if (StringUtils.isBlank(subscribe)) throw new DataTunnelException("subscribe 不能为空")

    val services = options.get("kafka.bootstrap.servers")
    if (StringUtils.isBlank(services)) throw new DataTunnelException("kafka.bootstrap.servers 不能为空")
  }

  override def read(sparkSession: SparkSession, options: util.Map[String, String]): Dataset[Row] = {
    val kafkaSouce = new KafkaSouce()
    val tmpTable = "tdl_datax_kafka_" + System.currentTimeMillis()
    kafkaSouce.createStreamTempTable(sparkSession, tmpTable, options)
    val sinkOptions: util.HashMap[String, String] = Maps.newHashMap()
    options.forEach((key, value) => {
      if (StringUtils.startsWith(key, "_sink_")) {
        sinkOptions.put(StringUtils.substringAfter(key, "_sink_"), value)
      }
    })

    val sinkType = options.get("__sinkType__")
    val sinkDatabaseName = sinkOptions.get("databaseName")
    val sinkTableName = sinkOptions.get("tableName")

    if ("hive" == sinkType) {
      if (!HudiUtils.isHudiTable(sparkSession, sinkTableName, sinkDatabaseName)) {
        throw new DataTunnelException(s"${sinkDatabaseName}.${sinkTableName} 不是hudi类型表")
      }
      val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
        "message, kafka_timestamp, date_format(timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable
      HudiUtils.deltaInsertStreamSelectAdapter(sparkSession, sinkDatabaseName, sinkTableName, querySql)
    } else if ("jdbc" == sinkType) {
      var connection: Connection = null
      try {
        val dsConf = sinkOptions.get("__dsConf__")
        val dsType = sinkOptions.get("__dsType__")
        val dsConfMap = MapperUtils.toJavaMap(dsConf)

        val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
          "message, kafka_timestamp, date_format(timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable

        var dataset = sparkSession.sql(querySql)
        val tdlName = "tdl_datax_" + System.currentTimeMillis
        dataset.createTempView(tdlName)

        var table = sinkTableName
        if (StringUtils.isNotBlank(sinkDatabaseName)) table = sinkDatabaseName + "." + sinkTableName

        val username = dsConfMap.get("username").asInstanceOf[String]
        var password = dsConfMap.get("password").asInstanceOf[String]
        password = AESUtil.decrypt(password)
        if (StringUtils.isBlank(username)) throw new IllegalArgumentException("username不能为空")
        if (StringUtils.isBlank(password)) throw new IllegalArgumentException("password不能为空")

        val url = JdbcUtils.buildJdbcUrl(dsType, dsConfMap)

        var batchsize = 1000
        if (sinkOptions.containsKey("batchsize")) batchsize = sinkOptions.get("batchsize").toInt
        var queryTimeout = 0
        if (sinkOptions.containsKey("queryTimeout")) queryTimeout = sinkOptions.get("queryTimeout").toInt

        val writeMode = sinkOptions.get("writeMode")
        var mode = SaveMode.Append
        if ("overwrite" == writeMode) mode = SaveMode.Overwrite

        val truncateStr = sinkOptions.get("truncate")
        var truncate = false
        if ("true" == truncateStr) truncate = true

        val sql = CommonUtils.genOutputSql(dataset, sinkOptions)
        dataset = sparkSession.sql(sql)

        val preSql = options.get("preSql")
        val postSql = options.get("postSql")
        if (StringUtils.isNotBlank(preSql) || StringUtils.isNotBlank(postSql)) {
          connection = buildConnection(url, table, options)
          sinkOptions.put("user", username)
        }

        if (StringUtils.isNotBlank(preSql)) {
          logInfo("exec preSql: " + preSql)
          JdbcUtils.execute(connection, preSql)
        }

        val checkpointLocation = s"/user/dataworks/stream_checkpoint/$sinkDatabaseName.db/$sinkTableName"
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
              .option("user", username)
              .option("password", password)
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
