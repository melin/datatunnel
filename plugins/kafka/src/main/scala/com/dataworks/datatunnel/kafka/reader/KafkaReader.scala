package com.dataworks.datatunnel.kafka.reader

import com.dataworks.datatunnel.api.DataxReader
import com.dataworks.datatunnel.api.{DataXException, DataxReader}
import com.dataworks.datatunnel.common.util.{AESUtil, CommonUtils, JdbcUtils}
import com.dataworks.datatunnel.kafka.util.HudiUtils
import com.gitee.melin.bee.util.MapperUtils
import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util
import scala.concurrent.duration.DurationInt

/**
 * huaixin 2021/12/29 2:23 PM
 */
class KafkaReader extends DataxReader {

  override def validateOptions(options: util.Map[String, String]): Unit = {
    val subscribe = options.get("subscribe")
    if (StringUtils.isBlank(subscribe)) throw new DataXException("subscribe 不能为空")

    val services = options.get("kafka.bootstrap.servers")
    if (StringUtils.isBlank(services)) throw new DataXException("kafka.bootstrap.servers 不能为空")
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
        throw new DataXException(s"${sinkDatabaseName}.${sinkTableName} 不是hudi类型表")
      }
      val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
        "message, kafka_timestamp, date_format(timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable
      HudiUtils.deltaInsertStreamSelectAdapter(sparkSession, sinkDatabaseName, sinkTableName, querySql)
    } else if ("jdbc" == sinkType) {
      val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
        "message, kafka_timestamp, date_format(timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable
      var dataset = sparkSession.sql(querySql)

      var table = sinkTableName
      if (StringUtils.isNotBlank(sinkDatabaseName)) table = sinkDatabaseName + "." + sinkTableName

      val username = sinkOptions.get("username")
      val password = sinkOptions.get("password")
      var url = sinkOptions.get("url")

      if (StringUtils.isBlank(username)) throw new IllegalArgumentException("username 不能为空")
      if (StringUtils.isBlank(password)) throw new IllegalArgumentException("password 不能为空")
      if (StringUtils.isBlank(url)) throw new IllegalArgumentException("url 不能为空")

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

      // https://stackoverflow.com/questions/2993251/jdbc-batch-insert-performance/10617768#10617768
      val dsType = sinkOptions.get("type")
      if ("mysql" == dsType) url = url + "?useServerPrepStmts=false&rewriteBatchedStatements=true&&tinyInt1isBit=false"
      else if ("postgresql" == dsType) url = url + "?reWriteBatchedInserts=true"


      val checkpointLocation = s"/user/dataworks/stream_checkpoint/$sinkDatabaseName.db/$sinkTableName"
      mkCheckpointDir(sparkSession, checkpointLocation)
      val query= dataset.writeStream
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
    } else {
      throw new UnsupportedOperationException("kafka 数据不支持同步到 " + sinkType)
    }

    null
  }

  private def mkCheckpointDir(sparkSession: SparkSession, path: String): Unit = {
    val configuration = sparkSession.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(configuration)
    if (!fs.exists(new Path(path))) fs.mkdirs(new Path(path))
  }
}
