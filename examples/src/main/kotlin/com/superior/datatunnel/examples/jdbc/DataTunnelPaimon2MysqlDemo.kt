package com.superior.datatunnel.examples.jdbc

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelPaimon2MysqlDemo {

    @JvmStatic
    fun main(args: Array<String>) {

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkGenericCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name + "," + PaimonSparkSessionExtensions::class.java.name)
            .getOrCreate()

        val sql = """
            DATATUNNEL SOURCE("hive") OPTIONS (
              databaseName = 'bigdata',
              tableName = 'paimon_orders_ods',
              columns = ["*"],
              sourceTempView='tdl_orders'
            ) 
            TRANSFORM = 'select k as value, v as name from tdl_orders'
            SINK("mysql") OPTIONS (
              username = "root",
              password = "Root2024!@",
              host = '172.18.6.181',
              port = 3306,
              databaseName = 'demos',
              tableName = 'paimon_orders',
              columns = ["*"]
            )
        """.trimIndent()

        spark.sql(sql)
    }
}