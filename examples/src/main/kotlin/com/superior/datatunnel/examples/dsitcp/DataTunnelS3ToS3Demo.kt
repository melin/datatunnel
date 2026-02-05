package com.superior.datatunnel.examples.dsitcp

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object DataTunnelS3ToS3Demo {

    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .enableHiveSupport()
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

            .config("spark.hadoop.fs.s3a.bucket.cyberengine.endpoint", "http://172.88.0.60:9000")
            .config("spark.hadoop.fs.s3a.bucket.cyberengine.access.key", "admin")
            .config("spark.hadoop.fs.s3a.bucket.cyberengine.secret.key", "rw9ZJaVU/bEN6IIk")
            .config("spark.hadoop.fs.s3a.bucket.cyberengine.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            .config("spark.hadoop.fs.s3a.bucket.test.endpoint", "http://172.18.6.181:9330")
            .config("spark.hadoop.fs.s3a.bucket.test.access.key", "minio")
            .config("spark.hadoop.fs.s3a.bucket.test.secret.key", "Minio2024")
            .config("spark.hadoop.fs.s3a.bucket.test.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            .getOrCreate()

        val sql = """
            DISTCP OPTIONS (
              srcPaths = ['s3a://cyberengine/common/aliyun-dlf/'],
              destPath = "s3a://test/spark1/",
              overwrite = true,
              consistentPathBehaviour = true,
              delete = true,
              excludeHiddenFile = true
            )
        """.trimIndent()

        spark.sql(sql)
    }
}