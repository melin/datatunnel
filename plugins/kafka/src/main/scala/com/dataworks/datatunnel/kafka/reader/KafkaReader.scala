package com.dataworks.datatunnel.kafka.reader

import com.dataworks.datatunnel.api.DataxReader
import com.dataworks.datatunnel.api.{DataXException, DataxReader}
import com.dataworks.datatunnel.kafka.util.HudiUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.hudi.HoodieSqlUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import java.util.UUID

/**
 * huaixin 2021/12/29 2:23 PM
 */
class KafkaReader extends DataxReader {

  override def validateOptions(options: util.Map[String, String]): Unit = {
    val subscribe = options.get("subscribe")
    if (StringUtils.isBlank(subscribe)) throw new DataXException("subscribe 不能为空")

    val services = options.get("kafka.bootstrap.servers")
    if (StringUtils.isBlank(services)) throw new DataXException("kafka.bootstrap.servers 不能为空")

    val targetDatabaseName = options.get("target_databaseName")
    if (StringUtils.isBlank(targetDatabaseName)) throw new DataXException("hive databaseName 不能为空")

    val targetTableName = options.get("target_tableName")
    if (StringUtils.isBlank(targetTableName)) throw new DataXException("hive tableName 不能为空")
  }

  override def read(sparkSession: SparkSession, options: util.Map[String, String]): Dataset[Row] = {
    val kafkaSouce = new KafkaSouce()
    val tmpTable = "tdl_datax_kafka_" + System.currentTimeMillis()
    kafkaSouce.createStreamTempTable(sparkSession, tmpTable, options)

    val targetDatabaseName = options.get("target_databaseName")
    val targetTableName = options.get("target_tableName")
    options.remove("target_databaseName")
    options.remove("target_tableName")

    if (!HudiUtils.isHudiTable(sparkSession, targetTableName, targetDatabaseName)) {
      throw new DataXException(s"${targetDatabaseName}.${targetTableName} 不是hudi类型表")
    }

    val querySql = "select if(kafka_key is not null, kafka_key, cast(kafka_timestamp as string)) as id, " +
      "message, unix_millis(kafka_timestamp) kafka_timestamp, date_format(kafka_timestamp, 'yyyyMMddHH') ds, kafka_topic from " + tmpTable
    HudiUtils.deltaInsertStreamSelectAdapter(sparkSession, targetDatabaseName, targetTableName, querySql)
    return null
  }

}
