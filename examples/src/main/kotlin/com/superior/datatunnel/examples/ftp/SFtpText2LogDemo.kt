package com.superior.datatunnel.examples.ftp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object SFtpText2LogDemo {
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
    protocol = 'sftp',
    host='172.18.5.46',
    port=22,
    username='test',
    password="test2023",
    format="csv",
    paths=["sftp:///ftpdata/csv/*.csv"])
SINK('log')
"""
        spark.sql(sql)
    }
}