package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelAwsPG2LogDemo {

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
            DATATUNNEL SOURCE("postgresql") OPTIONS (
              username = "postgres",
              password = "cider123456!",
              host = 'database-2.c8vfm34zprv8.us-east-1.rds.amazonaws.com',
              port = 5432,
              databaseName = 'cider',
              schemaName = 'public',
              tableName = 'catalog_returns',
              partitionColumn = 'cr_item_sk',
              numPartitions=20,
              columns = ["*"])
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}