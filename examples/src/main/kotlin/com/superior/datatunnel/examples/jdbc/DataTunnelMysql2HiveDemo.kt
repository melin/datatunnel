package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelMysql2HiveDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        var spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        println(spark.conf().get("spark.sql.parquet.int96RebaseModeInWrite"))
        val spark1 = spark.newSession()
        spark1.conf().set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

        println(spark1.conf().get("spark.sql.parquet.int96RebaseModeInWrite"))
        println(SparkSession.active().conf().get("spark.sql.parquet.int96RebaseModeInWrite"))
        println(SparkSession.getDefaultSession().get().conf().get("spark.sql.parquet.int96RebaseModeInWrite"))
        println(spark1.newSession().conf().get("spark.sql.parquet.int96RebaseModeInWrite"))
        println(spark.newSession().conf().get("spark.sql.parquet.int96RebaseModeInWrite"))

        val sql = """
            DATATUNNEL SOURCE("mysql") OPTIONS (
              username = "root",
              password = "root2023",
              host = '172.18.5.44',
              port = 3306,
              databaseName = 'demos',
              tableName = 'orders',
              columns = ["*"]
            ) 
            SINK("hive") OPTIONS (
              databaseName = "bigdata",
              tableName = 'ods_jdbc_orders',
              writeMode = 'overwrite',
              columns = ["*"]
            )
        """.trimIndent()

        spark1.sql(sql)
    }
}