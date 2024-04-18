package com.superior.datatunnel.examples.ftp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object FtpText2LogDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql = """
datatunnel SOURCE('ftp') OPTIONS(
    host='172.18.1.52',
    port=21,
    username='fcftp',
    password="fcftp",
    format="csv",
    paths=["ftp:///demo.csv"])
SINK('log')
"""
        spark.sql(sql)
    }
}