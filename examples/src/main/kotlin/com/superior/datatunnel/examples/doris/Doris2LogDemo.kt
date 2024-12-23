package com.superior.datatunnel.examples.doris

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession

object Doris2LogDemo {

    @JvmStatic
    fun main(args: Array<String>) {

        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.spark_catalog.uri", "thrift://cdh2:9083")
            .config(
                "spark.sql.extensions",
                DataTunnelExtensions::class.java.name + "," + IcebergSparkSessionExtensions::class.java.name
            )
            .getOrCreate()

        val sql = """
        DATATUNNEL SOURCE("doris") OPTIONS (
            feEnpoints="172.18.5.44:18030",
            username = "root",
            password = "",
            schemaName = "testdb",
            tableName = "table_hash",
            columns = ["*"]
        ) 
        SINK("log")""".trimIndent()

        spark.sql(sql)
    }

}