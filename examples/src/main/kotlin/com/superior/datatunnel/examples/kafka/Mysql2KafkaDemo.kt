package com.superior.datatunnel.examples.kafka

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2KafkaDemo {

    @JvmStatic
    fun main(args: Array<String>) {

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        // date_format(unix_millis(timestamp), 'yyyyMMddHH')
        val sql = """
            DATATUNNEL SOURCE("mysql") OPTIONS (
              username = "root",
              password = "root2023",
              host = '172.18.5.44',
              port = 3306,
              databaseName = 'demos',
              tableName = 'orders',
              columns = ["*"]
            ) 
            SINK("kafka") OPTIONS (
                topic = "demos",
                servers = "172.18.5.46:9092"
            )
        """.trimIndent()

        spark.sql(sql)
    }
}