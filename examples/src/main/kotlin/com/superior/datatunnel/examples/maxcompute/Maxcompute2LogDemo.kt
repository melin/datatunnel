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
                accessKeyId = 'LTAI5tHbjxR138YK5rQPq8kh',
                secretAccessKey = 'iW6U2iNbHSQPpIdYWYXSDcfGi5mPRM',
                columns = ["*"]
            ) 
            SINK("log")
        """.trimIndent()

        // spark.sql(sql)

        val sql1 = """
            DATATUNNEL SOURCE("maxcompute") OPTIONS (
              projectName = "superior",
              tableName = "orders",
              accessKeyId = 'LTAI5tHbjxR138YK5rQPq8kh',
              secretAccessKey = 'iW6U2iNbHSQPpIdYWYXSDcfGi5mPRM',
              endpoint='http://service.us-east-1.maxcompute.aliyun.com/api',
              columns = ["*"]
            )
            SINK("log")
        """.trimIndent()

        spark.sql(sql1)
    }
}