package com.superior.datatunnel.examples.kafka

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

object Kafka2KafkaDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name
                    + "," + HoodieSparkSessionExtension::class.java.name)
            .getOrCreate()

        // date_format(unix_millis(timestamp), 'yyyyMMddHH')
        val sql = """
            DATATUNNEL SOURCE("kafka") OPTIONS (
                format="json",
                subscribe = "users_json",
                servers = "172.18.5.46:9092",
                includeHeaders = true,
                sourceTempView='tdl_users',
                columns = ['id long', 'name string'],
                checkpointLocation = "/user/superior/stream_checkpoint/datatunnel/delta_users_kafka"
            )
            SINK("kafka") OPTIONS (
              topic = "users_json_1",
              servers = "172.18.5.46:9092"
            )
        """.trimIndent()

        spark.sql(sql)
    }
}