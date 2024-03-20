package com.superior.datatunnel.examples.hbase

import com.superior.datatunnel.core.DataTunnelExtensions
import org.apache.spark.sql.SparkSession

object Hbase2LogDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val spark = SparkSession
            .builder()
            .master("local")
            .appName("Datatunnel spark example")
            .config("spark.sql.extensions", DataTunnelExtensions::class.java.name)
            .getOrCreate()

        val sql = """
            datatunnel SOURCE('hbase') OPTIONS(
                tableName="default:test_10000w",
                zookeeperQuorum="node1,node2,node3",
                columns = ['col1 string cf1:col1', 'col2 string cf1:col2']
            )
            SINK('log')
        """
        spark.sql(sql)
    }
}