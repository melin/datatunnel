package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelPg2LogDemo {

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
            DATATUNNEL SOURCE('postgresql') OPTIONS (
                username = 'postgres',
                password='123456' ,
                host = '172.18.1.192',
                port = 5532 ,
                databaseName = 'postgres',
                schemaName = 'public',
                tableName = 'orders',
                columns = ["id", "customer_id", "amount", "created_at"],
                sourceTempView = 'tdl_table'
            ) 
            transform = "select id, customer_id customerId, amount, created_at createdAt from tdl_table"
            SINK("log")
        """.trimIndent()

        spark.sql(sql)
    }
}