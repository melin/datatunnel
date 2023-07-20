package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelAwsMysql2LogDemo {

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
              username = "admin",
              password = "xx",
              host = 'mysql.c8vfm34zprv8.us-east-1.rds.amazonaws.com',
              port = 3306,
              databaseName = 'wordpress',
              tableName = 'customer_address',
              partitionColumn = 'ca_address_sk',
              columns = ["*"])
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}