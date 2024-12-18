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
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .config("spark.ui.enabled", false)
            .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("kafka") OPTIONS (
                format="text",
                subscribe = "kafka_demos.ods_account",
                servers = "172.18.6.181:9092",
                'properties.kafka.security.protocol' = 'SASL_PLAINTEXT',
                'properties.kafka.sasl.mechanism' = 'PLAIN',
                'properties.kafka.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin2024";',
                includeHeaders = true,
                startingOffsets = 'earliest',
                sourceTempView='tdl_users',
                columns = ['*']
            ) 
            TRANSFORM = "select * from tdl_users"
            SINK("log")
        """.trimIndent()
        spark.sql(sql)
    }
}