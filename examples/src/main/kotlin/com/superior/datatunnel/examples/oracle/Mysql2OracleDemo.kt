package com.superior.datatunnel.examples.oracle

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2OracleDemo {

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
            Datatunnel source('mysql') OPTIONS(
                "username" = "root"
                ,"password" = "root2023"
                ,"jdbcUrl" = "jdbc:mysql://172.18.5.44:3306"
                ,"schemaName" = "demos"
                ,"tableName" = "test_1"
                ,"columns" = ["*"]
            )
            sink('oracle') OPTIONS(
                "username" = "system"
                ,"password" = "system"
                ,"jdbcUrl" = "jdbc:oracle:thin:@//172.18.1.51:1523/XE"
                ,"schemaName" = "FLINKUSER"
                ,"tableName" = "DEMOS"
                ,"writeMode" = "UPSERT"
                ,"upsertKeyColumns" = ["ID"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}