package com.dataworks.datatunnel.kafka.reader

import com.dataworks.datatunnel.api.DataxReader
import com.dataworks.datatunnel.api.{DataXException, DataxReader}
import com.dataworks.datatunnel.common.util.{AESUtil, CommonUtils, JdbcUtils}
import com.dataworks.datatunnel.kafka.util.HudiUtils
import com.gitee.melin.bee.util.MapperUtils
import com.google.common.collect.Maps
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.util

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
      if (options.containsKey("batchsize")) batchsize = options.get("batchsize").toInt
      var queryTimeout = 0
      if (options.containsKey("queryTimeout")) queryTimeout = options.get("queryTimeout").toInt

      val writeMode = options.get("writeMode")
      var mode = SaveMode.Append
      if ("overwrite" == writeMode) mode = SaveMode.Overwrite

      val truncateStr = options.get("truncate")
      var truncate = false
      if ("true" == truncateStr) truncate = true

      val sql = CommonUtils.genOutputSql(dataset, options)
      dataset = sparkSession.sql(sql)
      dataset.write.format("jdbc").mode(mode).option("url", url).option("dbtable", table).option("batchsize", batchsize).option("queryTimeout", queryTimeout).option("truncate", truncate).option("user", username).option("password", password).save()
    } else {
      throw new UnsupportedOperationException("kafka 数据不支持同步到 " + sinkType)
    }

    null
  }

}
