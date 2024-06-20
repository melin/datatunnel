package com.superior.datatunnel.examples.postgre

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2GreenPlumnCopyFromDemo {

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

        val sql1 = """
            DATATUNNEL SOURCE("mysql") OPTIONS (
              username = "root",
              password = "Datac@123",
              host = '172.18.1.55',
              port = 3306,
              databaseName = 'data_dev5.0.0',
              tableName = 'interface_call_log',
              columns = ["*"]
            ) 
            SINK("postgresql") OPTIONS (
              username = "gpadmin",
              password = "gpadmin",
              host = '172.18.1.190',
              port = 5432,
              truncate = true,
              databaseName = 'test',
              schemaName = 'public',
              tableName = 'interface_call_log',
              writeMode = 'bulkinsert',
              columns = ["*"])
        """.trimIndent()

        spark.sql(sql1)
    }
}