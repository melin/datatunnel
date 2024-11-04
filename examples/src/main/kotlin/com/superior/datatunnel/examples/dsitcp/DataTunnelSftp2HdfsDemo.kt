package com.superior.datatunnel.examples.dsitcp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelSftp2HdfsDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .config("spark.hadoop.fs.sftp.impl", "com.superior.datatunnel.hadoop.fs.sftp.SFTPFileSystem")
            .config("spark.hadoop.fs.sftp.host", "172.18.5.44")
            .config("spark.hadoop.fs.sftp.host.port", "22")
            .config("spark.hadoop.fs.sftp.user.172.18.5.44", "root")
            .config("spark.hadoop.fs.sftp.password.172.18.5.44.root", "123caoqwe")
            .config("spark.hadoop.fs.s3a.access.key", "McvVnpOziVsWv7Qlyut7")
            .config("spark.hadoop.fs.s3a.secret.key", "PbICbD6H7iyq0PuefHa383YoqJn3JCjedQHSYmbp")
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.18.6.181:9330/")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()

        val sql = """
            DISTCP OPTIONS (
              srcPaths = ['sftp://root@172.18.5.44/root'],
              destPath = "s3a://logs/",
              overwrite = true,
              delete = true,
              excludeHiddenFile = true
            )
        """.trimIndent()

        spark.sql(sql)
    }
}