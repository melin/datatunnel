package com.superior.datatunnel.examples.es

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Elasticsearch2LogDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql = """
datatunnel source('elasticsearch') OPTIONS(
    nodes='cdh1,cdh2,cdh3',
    port=9210,
    resource='spark/jobs')
SINK('log') OPTIONS(numRows = 10, truncate=0)
"""
        spark.sql(sql)
    }
}