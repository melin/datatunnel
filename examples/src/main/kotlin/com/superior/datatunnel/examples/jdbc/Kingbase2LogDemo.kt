package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Kingbase2LogDemo {

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
            DATATUNNEL SOURCE('kingbasees') OPTIONS (
                username = 'system',
                password = '12345678Ab!' ,
                jdbcUrl = 'jdbc:kingbase8://172.88.0.48:54321/sn_test',
                schemaName = 'public',
                tableName = 'kingbase_ALL_kingbase',
                columns = ["*"]
            ) 
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}