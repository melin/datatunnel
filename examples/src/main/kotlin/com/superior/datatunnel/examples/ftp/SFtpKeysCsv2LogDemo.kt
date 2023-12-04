package com.superior.datatunnel.examples.ftp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object SFtpKeysCsv2LogDemo {
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
    authType = 'sshkey',
    sshKeyFile = 'file:///Users/melin/datac.pem',
    sshPassphrase = 'passphrase',
    host='3.231.27.37',
    port=22,
    username='centos',
    format="csv",
    filePath="sftp:///home/centos/*.csv")
SINK('log')
"""
        spark.sql(sql)
    }
}