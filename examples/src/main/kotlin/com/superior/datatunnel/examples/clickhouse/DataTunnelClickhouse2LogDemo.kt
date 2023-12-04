package com.superior.datatunnel.examples.clickhouse

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelClickhouse2LogDemo {

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
            DATATUNNEL SOURCE("clickhouse") OPTIONS (
              username = "default",
              password = "2~8LpFFJT54eu",
              jdbcUrl = 'jdbc:clickhouse:https://fna4xzpegc.us-east-1.aws.clickhouse.cloud:8443?ssl=true&sslmode=STRICT',
              databaseName = 'default',
              tableName = '	test_ck_orders',
              columns = ["id", "username"]
            ) 
            SINK("log") OPTIONS (
              columns = ["id", "username"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}