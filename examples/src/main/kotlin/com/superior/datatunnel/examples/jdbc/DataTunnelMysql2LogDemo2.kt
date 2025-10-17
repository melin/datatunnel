package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelMysql2LogDemo2 {

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
            DATATUNNEL SOURCE("mysql") OPTIONS (
              username = "root",
              password = "Root2024!@",
              host = '172.18.6.181',
              port = 3306,
              schemaName = 'demos',
              tableName = 'paimon_orders',
              'properties.tinyInt1isBit' = 'true'
            ) 
            SINK("log") 
        """.trimIndent()

        spark.sql(sql)
    }
}