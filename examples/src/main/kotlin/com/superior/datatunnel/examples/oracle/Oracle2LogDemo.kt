package com.superior.datatunnel.examples.oracle

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Oracle2LogDemo {

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
            DATATUNNEL SOURCE("oracle") OPTIONS (
              username = "system",
              password = "system",
              jdbcUrl = 'jdbc:oracle:thin:@172.18.1.51:1523:XE',
              schemaName = 'FLINKUSER',
              tableName = 'DEMOS',
              columns = ["*"]
            )
            SINK("log") OPTIONS (
              columns = ["*"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}