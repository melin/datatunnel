package com.superior.datatunnel.examples.doris

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
import org.apache.spark.sql.SparkSession

object Mysql2DorisDemo {

    @JvmStatic
    fun main(args: Array<String>) {

        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                "spark.sql.extensions",
                DataTunnelExtensions::class.java.name + "," + IcebergSparkSessionExtensions::class.java.name
            )
            .getOrCreate()

        val sql = """
        DATATUNNEL SOURCE("mysql") OPTIONS (
              username = "root",
              password = "",
              host = '172.18.6.181',
              port = 3306,
              databaseName = 'demos',
              tableName = 'account',
              columns = ["*"]
        ) 
        SINK("doris") OPTIONS (
            feEnpoints="172.18.5.44:18030",
            username = "root",
            password = "",
            databaseName = "testdb",
            tableName = "ods_account"
        )
        """.trimIndent()

        spark.sql(sql)
    }

}