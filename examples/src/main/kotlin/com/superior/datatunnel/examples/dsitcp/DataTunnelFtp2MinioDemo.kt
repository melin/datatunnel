package com.superior.datatunnel.examples.dsitcp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelFtp2MinioDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .config("spark.hadoop.fs.ftp.impl", "com.superior.datatunnel.hadoop.fs.ftp.FTPFileSystem")
            .config("spark.hadoop.fs.ftp.host", "172.88.0.45")
            .config("spark.hadoop.fs.ftp.host.port", 21)
            .config("spark.hadoop.fs.ftp.user.172.88.0.45", "ftp")
            .config("spark.hadoop.fs.ftp.password.172.88.0.45.ftp", "ftp123")
            .config("spark.hadoop.fs.s3a.access.key", "McvVnpOziVsWv7Qlyut7")
            .config("spark.hadoop.fs.s3a.secret.key", "PbICbD6H7iyq0PuefHa383YoqJn3JCjedQHSYmbp")
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.18.6.181:9330/")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()

        val sql = """
            DISTCP OPTIONS (
              srcPaths = ['ftp://ftp@172.88.0.45/zichen_test/wxy'],
              destPath = "s3a://logs/",
              overwrite = true,
              consistentPathBehaviour = true,
              delete = true,
              excludeHiddenFile = true
            )
        """.trimIndent()

        spark.sql(sql)
    }
}