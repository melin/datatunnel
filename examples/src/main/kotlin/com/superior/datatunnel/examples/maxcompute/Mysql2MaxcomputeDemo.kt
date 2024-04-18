package com.superior.datatunnel.examples.maxcompute

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2MaxcomputeDemo {

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
                columns = ["userid", "age"],
                sourceTempView='tdl_users'
            ) 
            transform = "select userid as name, age, '20230605' as pt from tdl_users"
            SINK("maxcompute") options (
                projectName = "aloudata",
                tableName = "users",
                accessKeyId = '0rHycgWdKrPkIZpO',
                secretAccessKey = 'eAQR7Y4nJTViOarwiDRLHVJl78qs8M',
                partitionSpec = "pt='20230605'"
            )
        """.trimIndent()

        spark.sql(sql)
    }
}