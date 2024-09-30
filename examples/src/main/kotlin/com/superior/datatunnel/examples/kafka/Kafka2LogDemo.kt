package com.superior.datatunnel.examples.kafka

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Kafka2LogDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("kafka") OPTIONS (
                format="json",
                subscribe = "users_json",
                servers = "172.18.5.46:9092",
                includeHeaders = true,
                sourceTempView='tdl_users',
                columns = ['id long', 'name string']
            ) 
            TRANSFORM = "select id, name, date_format(kafka_timestamp, 'yyyMMdd') as ds from tdl_users"
            SINK("log")
        """.trimIndent()
        spark.sql(sql)
    }
}