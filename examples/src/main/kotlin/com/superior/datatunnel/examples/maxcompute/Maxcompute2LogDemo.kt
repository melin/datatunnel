package com.superior.datatunnel.examples.maxcompute

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Maxcompute2LogDemo {

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
            DATATUNNEL SOURCE("maxcompute") OPTIONS (
                projectName = "aloudata",
                tableName = "orders",
                accessKeyId = '0rHycgWdKrPkIZpO',
                secretAccessKey = 'eAQR7Y4nJTViOarwiDRLHVJl78qs8M',
                columns = ["*"]
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql)

        val sql1 = """
            DATATUNNEL SOURCE("maxcompute") OPTIONS (
                projectName = "aloudata",
                tableName = "users",
                accessKeyId = '0rHycgWdKrPkIZpO',
                secretAccessKey = 'eAQR7Y4nJTViOarwiDRLHVJl78qs8M',
                columns = ["*"],
                condition = "pt='20230605'"
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql1)
    }
}