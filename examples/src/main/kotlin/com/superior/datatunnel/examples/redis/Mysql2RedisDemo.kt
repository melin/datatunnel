package com.superior.datatunnel.examples.redis

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2RedisDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql = """
datatunnel source('mysql') OPTIONS(
    username='root',
    password='root2023',
    host='172.18.5.44',
    port=3306,
    databaseName='superior',
    tableName='meta_job',
    columns=['*'])
Sink('redis') OPTIONS(
    host='172.18.5.45',
    port=6379,
    database=1,
    table="meta_job",
    keyColumn="id",
    password='redis2023')
"""
        spark.sql(sql)
    }
}