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
              jdbcUrl = 'jdbc:mysql://172.18.6.181:3306/demos',
              schemaName = 'demos',
              tableName = 'paimon_orders',
              condition="id=2",
              columns = ['id', 'name', 'bool_test', 'flag']
            ) 
            SINK("log") 
        """.trimIndent()

        spark.sql(sql)
    }
}