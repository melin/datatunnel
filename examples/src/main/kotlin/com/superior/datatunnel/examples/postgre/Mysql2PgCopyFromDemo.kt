package com.superior.datatunnel.examples.postgre

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2PgCopyFromDemo {

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
              databaseName = '`data_dev4.4.0`',
              tableName = 'interface_call_log',
              columns = ["*"],
              partitionColumn = 'id'
            ) 
            SINK("postgresql") OPTIONS (
              username = "postgres",
              password = "postgres2023",
              host = '172.18.1.56',
              port = 5432,
              databaseName = 'postgres',
              schemaName = 'public',
              tableName = 'interface_call_log',
              writeMode = 'copyfrom',
              columns = ["*"])
        """.trimIndent()

        spark.sql(sql1)
    }
}