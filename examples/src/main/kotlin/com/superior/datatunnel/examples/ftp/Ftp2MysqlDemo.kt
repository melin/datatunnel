package com.superior.datatunnel.examples.ftp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Ftp2MysqlDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql = """
datatunnel SOURCE("ftp") OPTIONS(
    protocol = 'ftp',
    host='172.18.1.52',
    port=21,
    username='fcftp',
    password="fcftp",
    format="csv",
    paths=["/datatunnel/orders"])
SINK('mysql') OPTIONS (
    username = "root",
    password = "root2023",
    host = '172.18.5.44',
    port = 3306,
    databaseName = 'demos',
    tableName = 'orders',
    columns = ["*"],
    writeMode = 'UPSERT',
    upsertKeyColumns = ['id']
)
"""
        spark.sql(sql)
    }
}