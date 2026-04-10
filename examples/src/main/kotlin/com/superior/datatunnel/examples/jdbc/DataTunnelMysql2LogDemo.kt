package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelMysql2LogDemo {

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
                username = "root",
                password = "Root2024!@",
                host = '172.18.6.181',
                port = 3306,
                schemaName = 'demos',
                tableName = 'account_1',
                condition = "where 1=1 limit 1",
                sourceTempView = "tdl_view_2041389176958734337"
            ) 
            transform = "select '10002' as pt_admdvs, id, name, email, create_time from tdl_view_2041389176958734337"
            SINK("log") 
            OPTIONS (
                columns = ["pt_admdvs", "id", "name", "email", "create_time"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}