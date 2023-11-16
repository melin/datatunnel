package com.superior.datatunnel.examples.snowflake

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Snowflake2LogDemo {

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
            DATATUNNEL SOURCE("snowflake") OPTIONS (
                username = "melin",
                password = "xxxx",
                host = 'ipsliir-dpb29630.snowflakecomputing.com',
                databaseName = 'SNOWFLAKE_SAMPLE_DATA',
                schemaName = 'TPCH_SF1',
                tableName = 'ORDERS',
                columns = ["*"]
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}