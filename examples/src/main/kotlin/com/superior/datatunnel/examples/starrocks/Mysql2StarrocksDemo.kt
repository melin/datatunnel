package com.superior.datatunnel.examples.starrocks

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2StarrocksDemo {

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
                password = "root2023",
                host = '172.18.5.44',
                port = 3306,
                databaseName = 'demos',
                tableName = 'users',
                columns = ["*"],
                resultTableName='tdl_users'
            ) 
            transform = "select id, userid as username, age from tdl_users"
            SINK("starrocks") OPTIONS (
                "fe.http.url" = "172.18.1.190:8030",
                "fe.jdbc.url" = "jdbc:mysql://172.18.1.190:9030",
                tableName = 'test.mysql_users',
                user = 'root',
                password = "123456",
                "properties.partial_update" = "true" // starrocks.write.properties.*
            ) 
        """.trimIndent()

        spark.sql(sql)
    }
}