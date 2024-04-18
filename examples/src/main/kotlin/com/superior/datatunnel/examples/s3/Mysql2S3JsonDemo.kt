package com.superior.datatunnel.examples.s3

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2S3JsonDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
                .builder()
                .master("local")
                .appName("Datatunnel spark example")
                .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
                .getOrCreate()
        SparkSession.getActiveSession().get().conf().set("spark.jobserver.superior.jobType", "data_tunnel")

        val sql = """
datatunnel source('mysql') OPTIONS(
    username='root',
    password='root2023',
    host='172.18.5.44',
    port=3306,
    databaseName='superior',
    tableName='meta_job',
    columns=['*'])
Sink('s3') OPTIONS(
    region='us-east-1',
    accessKey='xx',
    secretKey='xxx',
    format="json",
    path="s3a://superior2023/datatunnel/demos.parquet")
"""
        spark.sql(sql)
    }
}