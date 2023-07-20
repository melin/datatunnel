package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension

object DataTunnelMysql2HudiDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name
                    + "," + HoodieSparkSessionExtension::class.java.name)
            .getOrCreate()

        spark.conf().set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        spark.conf().set("spark.sql.avro.datetimeRebaseModeInWrite", "LEGACY")

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
              tableName = 'ods_jdbc_orders_hudi',
              writeMode = 'overwrite',
              fileFormat = 'hudi',
              primaryKey = 'id',
              preCombineField = 'create_time',
              columns = ["*"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}