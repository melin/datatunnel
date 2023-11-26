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
                feEnpoints = "172.18.5.44:18030,172.18.5.45:18030,172.18.5.46:18030",
                jdbcUrl = "jdbc:mysql://172.18.5.44:9030/",
                databaseName = 'bigdata',
                tableName = 'account',
                username = 'root',
                password = "root2023"
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}