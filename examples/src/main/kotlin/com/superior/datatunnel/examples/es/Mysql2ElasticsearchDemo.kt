package com.superior.datatunnel.examples.es

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Mysql2ElasticsearchDemo {
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
Sink('elasticsearch') OPTIONS(
    nodes='cdh1,cdh2,cdh3',
    port=9210,
    resource='spark/jobs',
    indexKey='id')
"""
        spark.sql(sql)
    }
}