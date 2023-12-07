package com.superior.datatunnel.examples.s3

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object S3Excel2LogDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
                .builder()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                .getOrCreate()

        val sql = """
datatunnel SOURCE('s3') OPTIONS(
    endpoint='http://172.18.5.45:9300',
    accessKey='BxiljVd5YZa3mRUn',
    secretKey='3Mq9dsmdMbN1JipE1TlOF7OuDkuYBYpe',
    format="excel",
    filePath="s3a://demo-bucket/demo.xlsx",
    'properties.header' = true,
    'properties.inferSchema' = true)
SINK('log')
"""
        spark.sql(sql)
    }
}