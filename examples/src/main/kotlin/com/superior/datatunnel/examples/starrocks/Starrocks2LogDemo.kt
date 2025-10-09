package com.superior.datatunnel.examples.starrocks

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Starrocks2LogDemo {

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
            DATATUNNEL SOURCE("starrocks") OPTIONS (
                feEnpoints = "172.18.1.111:31149",
                jdbcUrl = "jdbc:mysql://172.18.1.111:32049/",
                databaseName = 'test_db',
                tableName = 'order_list',
                username = 'root',
                password = "root"
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}