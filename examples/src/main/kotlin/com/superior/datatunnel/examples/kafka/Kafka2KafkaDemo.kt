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
                subscribe = "orders",
                "kafka.bootstrap.servers" = "3.208.89.140:9092",
                includeHeaders = true,
                checkpointLocation = "/user/superior/stream_checkpoint/datatunnel/tdl_orders",
                resultTableName='tdl_users'
            )
            TRANSFORM = "select cast(timestamp as string) as key, 
                    concat(cast(value as string), '-2023') as value from tdl_users"
            SINK("kafka") OPTIONS (
              topic = "orders_dwd",
              "kafka.bootstrap.servers" = "3.208.89.140:9092"
            )
        """.trimIndent()

        spark.sql(sql)
    }
}