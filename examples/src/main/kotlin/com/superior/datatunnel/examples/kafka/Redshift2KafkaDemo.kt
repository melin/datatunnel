package com.superior.datatunnel.examples.kafka

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Redshift2KafkaDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val accessKeyId = "AKIAW77DWNKCQ6EV6AFI"
        val secretAccessKey = ""
        val iamRole = "arn:aws:iam::480976988805:role/service-role/AmazonRedshift-CommandsAccessRole-20230629T144155"

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
            DATATUNNEL SOURCE("redshift") OPTIONS (
                username = "admin",
                password = "Admin2024",
                jdbcUrl = 'jdbc:redshift://redshift-cluster-1.cvytjdhanbq8.us-east-1.redshift.amazonaws.com:5439/dev',
                schemaName = 'test',
                tableName = 'my_table',
                tempdir = 's3a://datacyber/redshift_temp/',
                region = 'us-east-1',
                accessKeyId = '${accessKeyId}',
                secretAccessKey = '${secretAccessKey}',
                iamRole = '${iamRole}',
                columns = ["*"]
            )
            SINK("kafka") OPTIONS (
                topic = "demos",
                servers = "172.18.5.146:9092"
                
            )
        """.trimIndent()

        spark.sql(sql)
    }
}