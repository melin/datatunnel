package com.superior.datatunnel.examples.snowflake

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2SnowflakeDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
                .builder()
                .master("local")
                .enableHiveSupport()
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("mysql") OPTIONS (
                user = "root",
                password = "root2023",
                host = '172.18.5.44',
                port = 3306,
                databaseName = 'demos',
                tableName = 'users',
                columns = ["*"],
                sourceTempView='tdl_users'
            ) 
            SINK("snowflake") OPTIONS (
                username = "",
                password = "",
                host = 'ipsliir-dpb29630.snowflakecomputing.com',
                databaseName = 'demos',
                schemaName = 'public',
                tableName = 'ORDERS',
                columns = ["*"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}