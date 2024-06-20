package com.superior.datatunnel.examples.postgre

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object GreenPlumn2MysqlDemo {

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
            DATATUNNEL SOURCE("greenplum") OPTIONS (
              username = "gpadmin",
              password = "gpadmin",
              host = '172.18.1.190',
              port = 5432,
              databaseName = 'test',
              schemaName = 'public',
              tableName = 'interface_call_log',
              columns = ["*"])
            SINK("mysql") OPTIONS (
              "username" = "root",
              "password" = "root2023",
              "jdbcUrl" = "jdbc:mysql://172.18.5.44:3306?allowLoadLocalInfile=true",
              "schemaName" = "demos.1.0",
              tableName = 'interface_call_log',
              writeMode = 'bulkinsert',
              columns = ["*"]
            ) 
        """.trimIndent()

        spark.sql(sql1)
    }
}