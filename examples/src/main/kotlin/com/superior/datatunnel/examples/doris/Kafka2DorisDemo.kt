package com.superior.datatunnel.examples.doris

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession

object Kafka2DorisDemo {

    @JvmStatic
    fun main(args: Array<String>) {

        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                "spark.sql.extensions",
                DataTunnelExtensions::class.java.name + "," + IcebergSparkSessionExtensions::class.java.name
            )
            .getOrCreate()

        val sql = """
        DATATUNNEL SOURCE("kafka") OPTIONS (
            format="text",
            subscribe = "doris_demos",
            servers = "172.18.6.181:9092",
            'properties.kafka.security.protocol' = 'SASL_PLAINTEXT',
            'properties.kafka.sasl.mechanism' = 'PLAIN',
            'properties.kafka.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin2024";',
            includeHeaders = false,
            startingOffsets = 'earliest',
            checkpointLocation = "/Users/melin/stream_checkpoint/datatunnel/doris_demos",
            columns = ['*']
        ) 
        SINK("doris") OPTIONS (
            feEnpoints="172.18.5.44:18030",
            username = "root",
            password = "",
            databaseName = "testdb",
            tableName = "table_hash",
            passthrough = true,
            fileFormat = "json",
            columns = ["k1", "k2", "k3", "k4"]
        )
        """.trimIndent()

        spark.sql(sql)
    }

}